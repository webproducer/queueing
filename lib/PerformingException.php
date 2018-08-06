<?php
namespace Queueing;

class PerformingException extends Exception
{
    use CreateFromExceptionTrait;

    private $needRepeat = false;
    private $repeatDelay = 0;
    /** @var JobInterface */
    private $job = null;

    public function getJob(): JobInterface
    {
        return $this->job;
    }

    public function setJob(JobInterface $job): self
    {
        $this->job = $job;
        return $this;
    }

    public function repeatAfter(int $delaySeconds = 0)
    {
        $this->needRepeat = true;
        $this->repeatDelay = $delaySeconds;
    }

    public function needsToBeRepeated()
    {
        return $this->needRepeat;
    }

    public function getRepeatDelay()
    {
        return $this->repeatDelay;
    }
}
