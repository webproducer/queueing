<?php

namespace Queueing\Backends\Beanstalk;

trait TimeoutTrait
{
    /**
     * @param int|null $timeout
     * @return int|null
     */
    private function millisecondsToSeconds(int $timeout = null)
    {
        if (!$timeout) { // null or zero
            return $timeout;
        }
        $timeout = intval(round($timeout / 1000));
        return $timeout < 1 ? 1 : $timeout;
    }
}
