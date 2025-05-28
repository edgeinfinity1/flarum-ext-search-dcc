<?php

/*
 * This file is part of blomstra/search.
 *
 * Copyright (c) 2022 Blomstra Ltd.
 *
 * For the full copyright and license information, please view the LICENSE.md
 * file that was distributed with this source code.
 *
 */

namespace Blomstra\Search\Api\Controllers;

use Blomstra\Search\Elasticsearch\MatchPhraseQuery;
use Blomstra\Search\Elasticsearch\MatchQuery;
use Blomstra\Search\Elasticsearch\TermsQuery;
use Blomstra\Search\Save\Document as ElasticDocument;
use Blomstra\Search\Searchers\Searcher;
use Elasticsearch\Client;
use Flarum\Api\Controller\ListDiscussionsController;
use Flarum\Api\Serializer\DiscussionSerializer;
use Flarum\Discussion\Discussion;
use Flarum\Extension\ExtensionManager;
use Flarum\Group\Group;
use Flarum\Http\RequestUtil;
use Flarum\Http\UrlGenerator;
use Flarum\Settings\SettingsRepositoryInterface;
use Flarum\User\User;
use Flarum\Tags\TagRepository;
use Illuminate\Contracts\Container\Container;
use Illuminate\Support\Arr;
use Illuminate\Support\Collection;
use Illuminate\Support\Str;
use Psr\Http\Message\ServerRequestInterface;
use ONGR\ElasticsearchDSL\Search;
use ONGR\ElasticsearchDSL\Query\Compound\BoolQuery as OngrBoolQuery;
use ONGR\ElasticsearchDSL\Query\Compound\FunctionScoreQuery;
use ONGR\ElasticsearchDSL\Query\FunctionScore\FieldValueFactorFunction;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermQuery as OngrTermQuery;
use ONGR\ElasticsearchDSL\Query\TermLevel\TermsQuery as OngrTermsQuery;
use ONGR\ElasticsearchDSL\Sort\FieldSort;
use ONGR\ElasticsearchDSL\Query\FullText\MatchQuery as OngrMatchQuery;
use ONGR\ElasticsearchDSL\Query\FullText\MatchPhraseQuery as OngrMatchPhraseQuery;
use Tobscure\JsonApi\Document;

class SearchController extends ListDiscussionsController
{
    public $serializer = DiscussionSerializer::class;

    protected array $translateSort = [
        'lastPostedAt' => 'updated_at',
        'createdAt'    => 'created_at',
        'commentCount' => 'comment_count',
        'view_count'   => 'view_count',
    ];

    protected Collection $searchers;
    protected TagRepository $tagrepo;
    protected bool $matchSentences;
    protected bool $matchWords;

    public function __construct(protected Client $elastic, protected UrlGenerator $uri, Container $container, SettingsRepositoryInterface $settings)
    {
        $this->searchers = $this->gatherSearchers($container->tagged('blomstra.search.searchers'));
        $this->tagrepo = new TagRepository();

        $this->matchSentences = true;
        $this->matchWords = true;
    }

    protected function gatherSearchers(iterable $searchers)
    {
        return collect($searchers)
            ->map(fn ($searcher) => new $searcher())
            ->filter(fn (Searcher $searcher) => $searcher->enabled());
    }

    protected function data(ServerRequestInterface $request, Document $document)
    {
        // Not used for now.
        $type = Arr::get($request->getQueryParams(), 'type');

        $actor = RequestUtil::getActor($request);

        $filters = $this->extractFilter($request);

        $search = $this->getSearch($filters);

        $limit = $this->extractLimit($request);
        
        $dyn_limit = $limit < 20 ? 20 : $limit;

        $offset = $this->extractOffset($request);

        $include = array_merge($this->extractInclude($request), ['state']);

        $tag = Arr::pull($request->getQueryParams(), 'filter.tag', '');
        $tag = $this->tagrepo->getIdForSlug($tag, $actor);

        $filterQuery = new OngrBoolQuery();  // MODIFIED: BoolQuery 替换

        // MODIFIED: 替换 Builder 为 Search
        $ongr_search = new Search();
        $ongr_search->setSize($limit + $offset + 1);
        $ongr_search->setFrom(0);
        $ongr_search->setMinScore(0);

        //echo($search);

        if (!empty($search)) {
            if ($this->matchSentences) {
                // MODIFIED: 适配新的查询结构
                $filterQuery->add($this->sentenceMatch($search), OngrBoolQuery::SHOULD);
            }
            if ($this->matchWords) {
                $filterQuery->add($this->wordMatch($search, 'and'), OngrBoolQuery::SHOULD);
                $filterQuery->add($this->wordMatch($search, 'or'), OngrBoolQuery::SHOULD);
            }
        }
        //echo(json_encode($request->getQueryParams()));
        if (!empty($tag)) {
            $tagFilter = new OngrTermQuery('tags', $tag);
            $filterQuery->add($tagFilter, OngrBoolQuery::FILTER);
        }

        // MODIFIED: 构建最终查询
        // $ongr_search->addQuery(
        //     $this->addFilters($filterQuery, $actor, $filters)
        // );

        $baseQuery = $this->addFilters($filterQuery, $actor, $filters);
        
        // 正确的新版写法：通过构造函数传递函数数组
        

        $sorts_all = [];
        foreach ($this->extractSort($request) as $field => $direction) {
            $field = $this->translateSort[$field] ?? $field;
            if ($field) {
                //echo($field);
                // $builder->addSort(new Sort($field, $direction));
                $ongr_search->addSort(new FieldSort($field, strtolower($direction) === 'desc' ? FieldSort::DESC : FieldSort::ASC));
                array_push($sorts_all, $field);
            } 
            if (!$sorts_all) {
                $mainQuery = new FunctionScoreQuery(
                    $baseQuery,      // 主查询
                );
        
                $mainQuery->addFieldValueFactorFunction(
                    'view_count',
                    0.1, 
                    'ln1p',
                    null, // 不再需要score_mode参数
                    0.1
                );
                // 将主查询设置到搜索对象
                $ongr_search->addQuery($mainQuery);
            } else {
                $ongr_search->addQuery($baseQuery);
            }
        }


        // $response = $builder->search();
        $dsl = $ongr_search->toArray();
        //echo(json_encode($dsl, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
        $response = $this->elastic->search([
            'index' => resolve('blomstra.search.elastic_index'),
            'body'  => $dsl
        ]);
        
        Discussion::setStateUser($actor);

        // Eager load groups for use in the policies (isAdmin check)
        if (in_array('mostRelevantPost.user', $include)) {
            $include[] = 'mostRelevantPost.user.groups';

            // If the first level of the relationship wasn't explicitly included,
            // add it so the code below can look for it
            if (!in_array('mostRelevantPost', $include)) {
                $include[] = 'mostRelevantPost';
            }
        }

        // Edge made this
        // function calculateWeight($views) {
        //     if ($views <= 0) return 0.0;
        //     $k = 0.0003389;
        //     $x0 = 7500;
        //     $steepness = $k * ($views - $x0);
        //     $base = 10 / (1 + exp(-$steepness));
        //     $transition = 10000;
        //     if ($views > $transition) {
        //         $slow_rate = ($views - $transition) * 0.00002222;
        //         $base = min($base + $slow_rate, 10.0);
        //     }
        //     return round(max(0, min($base, 10)), 2);
        // }        

        // we need to retrieve all discussion ids and when the results are posts,
        // their ids as most relevant post id
        $results = Collection::make(Arr::get($response, 'hits.hits'))
            ->map(function ($hit) {
                $type = $hit['_source']['type'];
                //echo($hit['_source']['type'].$hit['_source']['content']);
                $id = Str::after($hit['_source']['id'], "$type:");
                $view_count = 0;
                if (array_key_exists('view_count', $hit['_source'])) {
                    $view_count = $hit['_source']['view_count'];
                }
                if (!$view_count) {
                    $view_count = 1;
                }
                $score = Arr::get($hit, '_score');
                // $lpa = strtotime(Arr::get($hit, '_source.updated_at') || Arr::get($hit, '_source.created-at'));
                // $crt = strtotime(Arr::get($hit, '_source.created-at'));
                // $calc_weight = $score * calculateWeight($view_count);
                if ($type === 'posts') {
                    return [
                        'most_relevant_post_id' => $id,
                        'weight'                => $score,
                    ];
                } else {
                    return [
                        'discussion_id' => $id,
                        'weight'        => $score,
                    ];
                }
            });

        $document->addPaginationLinks(
            $this->uri->to('api')->route('blomstra.search', [
                'type' => 'discussions',
            ]),
            $request->getQueryParams(),
            $offset,
            $limit,
            $results->count() > $limit ? null : 0
        );

        //$results = $results->take($limit);
        //echo(count($results));

        $results_prior = $results->take($offset);

        $results_latter = $results->skip($offset)->take($limit);
        //echo($offset.$limit);
        
        $discard_discus_ids = [];
        //echo(count($results_prior));

        Discussion::query()
            ->select('discussions.*')
            ->join('posts', 'posts.discussion_id', 'discussions.id')
            // Extra safety to prevent leaking hidden discussion (titles) towards search results.
            ->when($actor->isGuest() || !$actor->hasPermission('discussion.hide'), fn ($query) => $query->whereNull('discussions.hidden_at'))
            ->where(function ($query) use ($results_prior) {
                $query
                    ->whereIn('discussions.id', $results_prior->pluck('discussion_id')->filter())
                    ->orWhereIn('posts.id', $results_prior->pluck('most_relevant_post_id')->filter());
            })
            ->get()
            ->each(function (Discussion $discussion) use (&$discard_discus_ids) {if (!in_array($discussion->id, $discard_discus_ids)) {array_push($discard_discus_ids, $discussion->id);}})
            ;
        //echo(implode(',',$discard_discus_ids));

        $discussions = Discussion::query()
            ->select('discussions.*')
            ->join('posts', 'posts.discussion_id', 'discussions.id')
            // Extra safety to prevent leaking hidden discussion (titles) towards search results.
            ->when($actor->isGuest() || !$actor->hasPermission('discussion.hide'), fn ($query) => $query->whereNull('discussions.hidden_at'))
            ->where(function ($query) use ($results_latter) {
                $query
                    ->whereIn('discussions.id', $results_latter->pluck('discussion_id')->filter())
                    ->orWhereIn('posts.id', $results_latter->pluck('most_relevant_post_id')->filter());
            })
            ->get()
            ->reject(function (Discussion $discussion) use ($discard_discus_ids) {return in_array($discussion->id, $discard_discus_ids);})
            ->each(function (Discussion $discussion) use ($results_latter, $sorts_all) {
                if (in_array($discussion->id, $results_latter->pluck('discussion_id')->toArray())) {
                    $discussion->most_relevant_post_id = $discussion->first_post_id;
                    $discussion->weight = $results_latter->firstWhere('discussion_id', $discussion->id)['weight'] ?? 0;
                } else {
                    $post = $discussion->posts()->whereIn('id', $results_latter->pluck('most_relevant_post_id'))->first();
                    $discussion->most_relevant_post_id = $post?->id ?? $discussion->first_post_id;
                    $discussion->weight = $results_latter->firstWhere('most_relevant_post_id', $post?->id)['weight'] ?? 0;
                }
                $filter_weight = 1;
                foreach($sorts_all as $sorting) {
                    //echo(strtotime($discussion->last_posted_at));
                    $discussion->filter_weight += match($sorting) {
                        'updated_at' => strtotime($discussion->last_posted_at ?: $discussion->created_at),
                        'created_at' => strtotime($discussion->created_at),
                        'comment_count' => $discussion->comment_count,
                        'view_count' => $discussion->view_count,
                    };
                    //echo($discussion->filter_weight);
                }
                //$discussion->
            })
            ->keyBy('id')
            ->when(!$sorts_all, function($discussions) {return $discussions->sortByDesc('weight');})
            ->when($sorts_all, function($discussions) {return $discussions->sortByDesc('filter_weight');})
            //->sortByDesc('weight')
            ->unique();
        // if ($limit < 20) {
        //     $discussions = $discussions->take($limit);
        // }
        //echo(count($discussions));

        $this->loadRelations($discussions, $include);

        if ($relations = array_intersect($include, ['firstPost', 'lastPost', 'mostRelevantPost'])) {
            foreach ($discussions as $discussion) {
                foreach ($relations as $relation) {
                    if ($discussion->$relation) {
                        $discussion->$relation->discussion = $discussion;
                    }
                }
            }
        }

        return $discussions;
    }

    protected function getDocument(string $type): ?ElasticDocument
    {
        $documents = resolve(Container::class)->tagged('blomstra.search.documents');

        return collect($documents)->first(function (ElasticDocument $document) use ($type) {
            return $document->type() === $type;
        });
    }

    protected function extensionEnabled(string $extension): bool
    {
        /** @var ExtensionManager $manager */
        $manager = resolve(ExtensionManager::class);

        return $manager->isEnabled($extension);
    }

    protected function addFilters($query, User $actor, array $filters = [])
    {
        $groups = $this->getGroups($actor);

        $onlyPrivate = Str::contains($filters['q'] ?? '', 'is:private');

        $tbool = new OngrBoolQuery();
        $tbool->add(new OngrTermQuery('is_private', 'false'), OngrBoolQuery::FILTER);
        $tbool->add(new OngrTermsQuery('groups', $groups->toArray()), OngrBoolQuery::FILTER);
        $subQuery = $tbool;


        if ($this->extensionEnabled('fof-byobu') && $actor->exists) {

            $byobuQuery = (new OngrBoolQuery())
                ->add(new OngrTermQuery('is_private', 'true'), OngrBoolQuery::FILTER)
                ->add(
                    (new OngrBoolQuery())
                        ->add(new OngrTermsQuery('recipient_groups', $groups->toArray()), OngrBoolQuery::SHOULD)
                        ->add(new OngrTermsQuery('recipient_users', [$actor->id]), OngrBoolQuery::SHOULD),
                    OngrBoolQuery::FILTER
                );

            if ($onlyPrivate) {
                $subQuery = $byobuQuery;
            } else {
                $subQuery = (new OngrBoolQuery())
                    ->add($subQuery, OngrBoolQuery::SHOULD)
                    ->add($byobuQuery, OngrBoolQuery::SHOULD);
            }


        }

        $query->add($subQuery,OngrBoolQuery::FILTER);

        return $query;
    }

    protected function boolQuery($parent, float $boost = 1): OngrBoolQuery
    {
        $bool = new OngrBoolQuery();

        /** @var Searcher $searcher */
        foreach ($this->searchers as $searcher) {
            $searcher = new $searcher();

            $tbool = new OngrBoolQuery();
            $tbool->add(new OngrTermQuery('type', $searcher->type()), OngrBoolQuery::FILTER);
            $tbool->add($parent->setParameters(['boost' => $boost * $searcher->boost()]));
            $bool->add(
                $tbool
                , // MODIFIED: 设置 boost 方式
                OngrBoolQuery::SHOULD
            );

        }

        return $bool;
    }

    protected function sentenceMatch(string $q): OngrBoolQuery
    {

        $query = new OngrMatchPhraseQuery('content', $q);
        return $this->boolQuery($query, 0.4);
    }

    protected function wordMatch(string $q, string $operator = 'or'): OngrBoolQuery
    {

        $query = new OngrMatchQuery('content', $q, ['operator' => $operator]);
        $boost = $operator === 'and' ? 1.8 : 0.8;
        return $this->boolQuery($query, $boost);

    }

    protected function getGroups(User $actor): Collection
    {
        /** @var Collection $groups */
        $groups = $actor->groups->pluck('id');

        $groups->add(Group::GUEST_ID);

        if ($actor->is_email_confirmed) {
            $groups->add(Group::MEMBER_ID);
        }

        return $groups;
    }

    protected function getSearch(array $filters): ?string
    {
        $search = Arr::get($filters, 'q');

        if ($search) {
            $q = collect(explode(' ', $search))
                ->filter(function (string $part) {
                    return $part !== 'is:private';
                })
                ->filter()
                ->join(' ');

            return empty($q) ? null : $q;
        }

        return null;
    }
}
