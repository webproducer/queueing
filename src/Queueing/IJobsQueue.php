<?php
namespace Queueing;


interface IJobsQueue
{

    const DEFAULT_TTR = 600; // 10 mins
    const DEFAULT_PRI = 1;

    /**
     * @param int|null $timeout
     * @return array - [$jobId, $payload]
     */
    function reserve(int $timeout = null): array;

    /**
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
     * @param int $id
     */
    function delete(int $id);

    /**
     * @param int $id
     */
    function bury(int $id);

}
