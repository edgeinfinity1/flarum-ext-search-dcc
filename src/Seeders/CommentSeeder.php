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

namespace Blomstra\Search\Seeders;

use Blomstra\Search\Save\Document;
use Flarum\Api\Serializer\PostSerializer;
use Flarum\Post\CommentPost;
use Flarum\Post\Event as Core;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;

class CommentSeeder extends Seeder
{
    public function type(): string
    {
        return resolve(PostSerializer::class)->getType(new CommentPost());
    }

    public function query(): Builder
    {
        $includes = ['discussion'];

        if ($this->extensionEnabled('flarum-tags')) {
            $includes[] = 'discussion.tags';
        }

        if ($this->extensionEnabled('fof-byobu')) {
            $includes[] = 'discussion.recipientUsers';
            $includes[] = 'discussion.recipientGroups';
        }

        return CommentPost::query()
            ->whereNull('hidden_at')
            ->where('type', CommentPost::$type)
            ->with($includes);
    }

    public static function savingOn(Dispatcher $events, callable $callable)
    {
        $events->listen([
            Core\Posted::class,
        ], function ($event) use ($callable) {
            $callable($event->post);
        });
    }

    public static function deletingOn(Dispatcher $events, callable $callable)
    {
        $events->listen([
            Core\Deleted::class
        ], function ($event) use ($callable) {
            $callable($event->post);
        });
    }

    /**
     * @param CommentPost $model
     *
     * @return Document
     */
    public function toDocument(Model $model): Document
    {
        $document = new Document([
            'type'            => $this->type(),
            'id'              => $this->type().':'.$model->id,
            'rawId'           => $model->id,
            'content'         => $model->content,
            'content_partial' => $model->content,
            'created_at'      => $model->created_at?->toAtomString(),
            'updated_at'      => $model->edited_at?->toAtomString(),
            'is_private'      => $model->is_private,
            'user_id'         => $model->user_id,
            'groups'          => $this->groupsForDiscussion($model->discussion),
            'comment_count'   => $model->discussion->comment_count,
        ]);
        
        if (true) {
            $document['view_count'] = $model->discussion->view_count;
        }

        if ($this->extensionEnabled('flarum-tags')) {
            $document['tags'] = $model->discussion->tags->pluck('id')->toArray();
        }

        if ($this->extensionEnabled('fof-byobu')) {
            $document['recipient_users'] = $model->discussion->recipientUsers
                ->whereNull('removed_at')
                ->pluck('id')
                ->toArray();
            $document['recipient_groups'] = $model->discussion->recipientGroups
                ->whereNull('removed_at')
                ->pluck('id')
                ->toArray();
        }

        return $document;
    }
}
