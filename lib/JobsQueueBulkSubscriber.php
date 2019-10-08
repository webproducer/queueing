<?php

namespace Queueing;

use function Amp\asyncCall;

/**
 * Class JobsQueueBulkSubscriber
 */
class JobsQueueBulkSubscriber extends AbstractJobsQueueSubscriber
{
    private $portion = 1;

    public function setBulkSize(int $size): self
    {
        $this->portion = $size;
        return $this;
    }

    /**
     * @param int $milliseconds
     * @return JobsQueueBulkSubscriber
     * @deprecated Use 'setMaxWaitTime' instead
     */
    public function setBulkMaxWaitTime(int $milliseconds): self
    {
        $this->setMaxWaitTime($milliseconds);
        return $this;
    }

    public function subscribe(): Subscription
    {
        asyncCall(function () {
            $jobs = [];
            $jobsCount = 0;
            while ($jobData = yield $this->nextJob($this->waitTime)) {
                $isTimedOut = $jobData === self::TIMED_OUT;
                if (!$isTimedOut) {
                    $jobs[] = $jobData;
                    ++$jobsCount;
                }
                if ($jobsCount && (($jobsCount === $this->portion) || $isTimedOut)) {
                    yield $this->emit($this->makeList($jobs));
                    $jobs = [];
                    $jobsCount = 0;
                }
                yield $this->processResults();
            }
            if (!empty($jobs)) {
                yield $this->emit($this->makeList($jobs));
            }
            yield $this->complete();
        });
        return $this->makeSubscription();
    }

    private function makeList(array $jobDescs): Bulk
    {
        return new Bulk(array_map(function ($jobDesc) {
            //TODO: handle JobCreatingException?
            return $this->makeJob($jobDesc);
        }, $jobDescs));
    }
}
