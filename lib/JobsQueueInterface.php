<?php

namespace Queueing;

use Amp\Promise;

interface JobsQueueInterface
{
    const DEFAULT_TTR = 600; // 10 mins
    const DEFAULT_PRI = 1;

    /**
     * Wait [timeout] for ready job to reserve
     *
     * @param int|null $timeout Milliseconds
     * @return array|Promise - [$jobId, $payload] (or Promise that wiil be resolved with same array)
     */
    public function reserve(int $timeout = null);

    /**
     * Add new job
     *
     * @param string $payload
     * @param int $priority
     * @param int $delaySeconds
     * @param int $ttr
     * @return int|Promise - Created Job id (or Promise that will be resolved with id)
     */
    public function add(
        string $payload,
        int $priority = self::DEFAULT_PRI,
        int $delaySeconds = 0,
        int $ttr = self::DEFAULT_TTR
    );

    /**
     * Return job back to queue
     *
     * @param int $id
     * @param int $delaySeconds
     * @return void|Promise
     */
    public function release(int $id, int $delaySeconds = 0);

    /**
     * Delete job
     *
     * @param int $id
     * @return void|Promise
     */
    public function delete(int $id);

    /**
     * Bury (fail) job
     *
     * @param int $id
     * @return void|Promise
     */
    public function bury(int $id);
}
