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

use Blomstra\Search\Save\Document as ElasticDocument;
use Blomstra\Search\Searchers\Searcher;
use Elasticsearch\Client;
use Flarum\Api\Controller\ListDiscussionsController;
use Flarum\Api\Serializer\DiscussionSerializer;
use Flarum\Discussion\Discussion;
use Flarum\Extension\ExtensionManager;
use Flarum\Http\RequestUtil;
use Flarum\Http\UrlGenerator;
use Flarum\Settings\SettingsRepositoryInterface;
use Flarum\Tags\TagRepository;
use Flarum\User\User;
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

        $searchParsing = $this->parseSearchFilters($filters, $actor);
        $search = $searchParsing['search'];

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

        //echo($search);

        if (!empty($search)) {
            $ongr_search->setMinScore(0.2);

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

        $this->applyParsedFilters($filterQuery, $searchParsing['filters']);

        $baseQuery = $filterQuery;
        
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
        }

        if (!$sorts_all) {
            $mainQuery = new FunctionScoreQuery($baseQuery);

            $mainQuery->addScriptScoreFunction(
                "
                    double x = doc['view_count'].size() == 0 ? 0 : doc['view_count'].value;
                    double popularity = Math.log(1 + x);
                    return _score * (1 + popularity);
                "
            );

            // 将主查询设置到搜索对象
            $ongr_search->addQuery($mainQuery);
        } else {
            $ongr_search->addQuery($baseQuery);
        }

        // $response = $builder->search();
        $dsl = $ongr_search->toArray();
        //echo(json_encode($dsl, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
        $response = $this->elastic->search([
            'index' => resolve('blomstra.search.elastic_index'),
            'body'  => $dsl
        ]);
        
        Discussion::setStateUser($actor);

        if (in_array('mostRelevantPost.user', $include)) {
            $include[] = 'mostRelevantPost.user.groups';

            // If the first level of the relationship wasn't explicitly included,
            // add it so the code below can look for it
            if (!in_array('mostRelevantPost', $include)) {
                $include[] = 'mostRelevantPost';
            }
        }

        $results = Collection::make(Arr::get($response, 'hits.hits'))
            ->map(function ($hit) {
                $type = $hit['_source']['type'];
                //echo($hit['_source']['type'].$hit['_source']['content']);
                $id = Str::after($hit['_source']['id'], "$type:");
                $score = Arr::get($hit, '_score');

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
            ->whereVisibleTo($actor)
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
            ->whereVisibleTo($actor)
            // Extra safety to prevent leaking hidden discussion (titles) towards search results.
            ->when($actor->isGuest() || !$actor->hasPermission('discussion.hide'), fn ($query) => $query->whereNull('discussions.hidden_at'))
            ->where(function ($query) use ($results_latter) {
                $query
                    ->whereIn('discussions.id', $results_latter->pluck('discussion_id')->filter())
                    ->orWhereIn('posts.id', $results_latter->pluck('most_relevant_post_id')->filter());
            })
            ->get()
            ->reject(function (Discussion $discussion) use ($discard_discus_ids) {return in_array($discussion->id, $discard_discus_ids);})
            ->each(function (Discussion $discussion) use ($results_latter, $sorts_all, $actor) {
                if (in_array($discussion->id, $results_latter->pluck('discussion_id')->toArray())) {
                    $discussion->most_relevant_post_id = $discussion->first_post_id;
                    $discussion->weight = $results_latter->firstWhere('discussion_id', $discussion->id)['weight'] ?? 0;
                } else {
                    $post = $discussion->posts()
                        ->whereVisibleTo($actor)
                        ->whereIn('id', $results_latter->pluck('most_relevant_post_id'))
                        ->first();
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
        return $this->boolQuery($query, 0.1);
    }

    protected function wordMatch(string $q, string $operator = 'or'): OngrBoolQuery
    {

        $query = new OngrMatchQuery('content', $q, ['operator' => $operator]);
        $boost = $operator === 'and' ? 2.0 : 1.0;
        return $this->boolQuery($query, $boost);

    }

    protected function parseSearchFilters(array $filters, $actor): array
    {
        $search = Arr::get($filters, 'q');

        if (!$search) {
            return ['search' => null, 'filters' => []];
        }

        $parsedFilters = [];
        $freeText = [];

        foreach (explode(' ', $search) as $part) {
            $part = trim($part);

            if ($part === '') {
                continue;
            }

            if (preg_match('/^#(\d+)$/', $part, $matches) === 1) {
                $parsedFilters['discussion_ids'][] = (int) $matches[1];
                continue;
            }

            if (preg_match('/^(?:id|discussion):(\d+)$/', $part, $matches) === 1) {
                $parsedFilters['discussion_ids'][] = (int) $matches[1];
                continue;
            }

            if (preg_match('/^(?:user|author):(.+)$/', $part, $matches) === 1) {
                $parsedFilters['user_ids'] = array_merge(
                    $parsedFilters['user_ids'] ?? [],
                    $this->resolveUserIds(explode(',', $matches[1]))
                );
                continue;
            }

            if (preg_match('/^tag:(.+)$/', $part, $matches) === 1) {
                $parsedFilters['tag_ids'] = array_merge(
                    $parsedFilters['tag_ids'] ?? [],
                    $this->resolveTagIds(explode(',', $matches[1]), $actor)
                );
                continue;
            }

            if ($part === 'is:private') {
                $parsedFilters['is_private'] = true;
                continue;
            }

            if ($part === 'is:public') {
                $parsedFilters['is_private'] = false;
                continue;
            }

            if ($part === 'is:sticky') {
                $parsedFilters['is_sticky'] = true;
                continue;
            }

            if ($part === 'is:stickiest') {
                $parsedFilters['is_stickiest'] = true;
                continue;
            }

            $freeText[] = $part;
        }

        return [
            'search'  => empty($freeText) ? null : implode(' ', $freeText),
            'filters' => $parsedFilters,
        ];
    }

    protected function resolveUserIds(array $references): array
    {
        $references = collect($references)
            ->map(fn (string $reference) => trim($reference))
            ->filter()
            ->unique()
            ->values();

        $numericIds = $references
            ->filter(fn (string $reference) => ctype_digit($reference))
            ->map(fn (string $reference) => (int) $reference)
            ->values()
            ->all();

        $usernames = $references
            ->reject(fn (string $reference) => ctype_digit($reference))
            ->values();

        if ($usernames->isNotEmpty()) {
            $idsByUsername = User::query()
                ->whereIn('username', $usernames->all())
                ->pluck('id')
                ->all();

            $numericIds = array_merge($numericIds, $idsByUsername);
        }

        return array_values(array_unique($numericIds));
    }

    protected function resolveTagIds(array $references, $actor): array
    {
        $references = collect($references)
            ->map(fn (string $reference) => trim($reference))
            ->filter()
            ->unique()
            ->values();

        $tagIds = [];

        foreach ($references as $reference) {
            if (ctype_digit($reference)) {
                $tagIds[] = (int) $reference;
                continue;
            }

            $id = $this->tagrepo->getIdForSlug($reference, $actor);

            if ($id) {
                $tagIds[] = (int) $id;
            }
        }

        return array_values(array_unique($tagIds));
    }

    protected function applyParsedFilters(OngrBoolQuery $bool, array $filters): void
    {
        if (!empty($filters['discussion_ids'])) {
            $discussionIdQuery = new OngrBoolQuery();
            $discussionIdQuery->add(new OngrTermQuery('type', 'discussions'), OngrBoolQuery::FILTER);

            foreach (array_unique($filters['discussion_ids']) as $discussionId) {
                $discussionIdQuery->add(new OngrTermQuery('rawId', $discussionId), OngrBoolQuery::SHOULD);
            }

            $discussionIdQuery->setParameters(['minimum_should_match' => 1]);

            $bool->add($discussionIdQuery, OngrBoolQuery::FILTER);
        }

        if (!empty($filters['user_ids'])) {
            $userQuery = new OngrBoolQuery();

            foreach (array_unique($filters['user_ids']) as $userId) {
                $userQuery->add(new OngrTermQuery('user_id', $userId), OngrBoolQuery::SHOULD);
            }

            $userQuery->setParameters(['minimum_should_match' => 1]);

            $bool->add($userQuery, OngrBoolQuery::FILTER);
        }

        if (!empty($filters['tag_ids'])) {
            $tagQuery = new OngrBoolQuery();

            foreach (array_unique($filters['tag_ids']) as $tagId) {
                $tagQuery->add(new OngrTermQuery('tags', $tagId), OngrBoolQuery::SHOULD);
            }

            $tagQuery->setParameters(['minimum_should_match' => 1]);

            $bool->add($tagQuery, OngrBoolQuery::FILTER);
        }

        if (array_key_exists('is_private', $filters)) {
            $bool->add(new OngrTermQuery('is_private', $filters['is_private']), OngrBoolQuery::FILTER);
        }

        if (!empty($filters['is_sticky'])) {
            // To make it native-like
            $stickyQuery = new OngrBoolQuery();
            $stickyQuery->add(new OngrTermQuery('is_sticky', true), OngrBoolQuery::SHOULD);
            $stickyQuery->add(new OngrTermQuery('is_stickiest', true), OngrBoolQuery::SHOULD);
            $stickyQuery->setParameters(['minimum_should_match' => 1]);
            $bool->add($stickyQuery, OngrBoolQuery::FILTER);
        }

        if (!empty($filters['is_stickiest'])) {
            $bool->add(new OngrTermQuery('is_stickiest', true), OngrBoolQuery::FILTER);
        }
    }
}
