<?php
namespace Queueing;


class PerformingException extends Exception
{
    private $needRepeat = false;
    private $repeatDelay = 0;

    public function repeatAfter(int $delaySeconds = 0) {
        $this->needRepeat = true;
        $this->repeatDelay = $delaySeconds;
    }

    public function needsToBeRepeated() {
        return $this->needRepeat;
    }

    public function getRepeatDelay() {
        return $this->repeatDelay;
    }

}
