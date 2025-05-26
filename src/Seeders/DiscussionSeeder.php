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
use Flarum\Api\Serializer\DiscussionSerializer;
use Flarum\Discussion\Discussion;
use Flarum\Discussion\Event as Core;
use FoF\Byobu\Events as Byobu;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;

class DiscussionSeeder extends Seeder
{
    public function type(): string
    {
        return resolve(DiscussionSerializer::class)->getType(new Discussion());
    }

    public function query(): Builder
    {
        $includes = [];

        if ($this->extensionEnabled('flarum-tags')) {
            $includes[] = 'tags';
        }

        if ($this->extensionEnabled('fof-byobu')) {
            $includes[] = 'recipientUsers';
            $includes[] = 'recipientGroups';
        }

        return Discussion::query()
            ->whereNull('hidden_at')
            ->with($includes);
    }

    public static function savingOn(Dispatcher $events, callable $callable)
    {
        $events->listen([
            // flarum/core events
            Core\Started::class, Core\Restored::class,
            // fof/byobu discussion recipients events.
            Byobu\DiscussionMadePublic::class, Byobu\RemovedSelf::class, Byobu\RecipientsChanged::class,
        ], function ($event) use ($callable) {
            return $callable($event->discussion);
        });
    }

    public static function deletingOn(Dispatcher $events, callable $callable)
    {
        $events->listen([
            // flarum/core events.
            Core\Deleted::class, Core\Hidden::class
        ], function ($event) use ($callable) {
            return $callable($event->discussion);
        });
    }

    /**
     * @param Discussion $model
     *
     * @return Document
     */
    public function toDocument(Model $model): Document
    {
        $document = new Document([
            'type'            => $this->type(),
            'id'              => $this->type().':'.$model->id,
            'rawId'           => $model->id,
            'content'         => $model->title,
            'content_partial' => $model->title,
            'created_at'      => $model->created_at?->toAtomString(),
            'updated_at'      => $model->last_posted_at?->toAtomString(),
            'is_private'      => $model->is_private,
            'user_id'         => $model->user_id,
            'groups'          => $this->groupsForDiscussion($model),
            'comment_count'   => $model->comment_count,
        ]);
        
        if (true) {
            $document['view_count'] = $model->view_count;
        }

        if ($this->extensionEnabled('flarum-tags')) {
            $document['tags'] = $model->tags->pluck('id')->toArray();
        }

        if ($this->extensionEnabled('fof-byobu')) {
            $document['recipient_users'] = $model->recipientUsers
                ->whereNull('removed_at')
                ->pluck('id')
                ->toArray();
            $document['recipient_groups'] = $model->recipientGroups
                ->whereNull('removed_at')
                ->pluck('id')
                ->toArray();
        }

        if ($this->extensionEnabled('flarum-sticky')) {
            $document['is_sticky'] = (bool) $model->is_sticky;
        }

        return $document;
    }
}
