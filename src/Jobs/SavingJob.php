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

namespace Blomstra\Search\Jobs;

use Blomstra\Search\Exceptions\SeedingException;
use Elasticsearch\Client;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Arr;

class SavingJob extends Job
{
    public function handle(Client $client)
    {
        if ($this->models->isEmpty()) {
            return;
        }

        // Preparing body for storing.
        $body = $this->models->flatMap(function (Model $model) {
            $document = $this->seeder->toDocument($model);

            $failed = $document->failed ? true : false;
            if ($failed) { 
                return [];
            }

            return [
                ['index' => ['_index' => $this->index, '_id' => $document->id]],
                $document->toArray(),
            ];
        });
        //echo(json_encode($body));
        // ->flatten(1);

        if ($body->isEmpty()) {
            return;
        }

        $response = $client->bulk([
            'index'   => $this->index,
            'body'    => $body->toArray(),
            'refresh' => true,
        ]);

        if (Arr::get($response, 'errors') !== true) {
            return true;
        }

        $items = Arr::get($response, 'items');

        $error = Arr::get(Arr::first($items), 'index.error.reason');

        throw new SeedingException(
            "Failed to seed: $error",
            $items
        );
    }
}
