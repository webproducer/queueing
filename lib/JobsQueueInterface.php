<?php
namespace Queueing;


interface JobsQueueInterface
{

    const DEFAULT_TTR = 600; // 10 mins
    const DEFAULT_PRI = 1;

    /**
     * Wait [timeout] for ready job to reserve
     *
     * @param int|null $timeout
     * @return array - [$jobId, $payload]
     */
    function reserve(int $timeout = null): array;

    /**
     * Add new job
     *
     * @param string $payload
     * @param int $priority
     * @param int $delaySeconds
     * @param int $ttr
     * @return int - Created Job id
     */
    function add(
        string $payload,
        int $priority = self::DEFAULT_PRI,
        int $delaySeconds = 0,
        int $ttr = self::DEFAULT_TTR
    ): int;

    /**
     * Return job back to queue
     *
     * @param int $id
     * @param int $delaySeconds
     */
    function release(int $id, int $delaySeconds = 0);

    /**
     * Delete job
     *
     * @param int $id
     */
    function delete(int $id);

    /**
     * Bury (fail) job
     *
     * @param int $id
     */
    function bury(int $id);

}
