<?php
namespace Queueing;

use function Amp\asyncCall;

/**
 * Class JobsQueueBulkSubscriber
 * @package Queueing
 */
class JobsQueueBulkSubscriber extends AbstractJobsQueueSubscriber
{

    private $portion = 1;
    private $waitTime = 0;

    public function setBulkSize(int $size): self
    {
        $this->portion = $size;
        return $this;
    }

    public function setBulkMaxWaitTime(int $milliseconds): self
    {
        $this->waitTime = $milliseconds;
        return $this;
    }

    public function subscribe(): Subscription
    {
        asyncCall(function () {
            //TODO: use timeout to cancel Promise returned by reserve() on exiting?
            $jobs = [];
            while ($jobData = yield $this->nextJob()) {
                $lastJobAt = $this->getMoment();
                $jobs[] = $jobData;
                if ($this->isReadyToEmit(count($jobs), $lastJobAt)) {
                    yield $this->emitAndProcess($this->makeList($jobs));
                    $jobs = [];
                }
            }
            if (!empty($jobs)) {
                yield $this->emitAndProcess($this->makeList($jobs));
            }
            $this->complete();
        });
        return $this->makeSubscription();
    }

    private function isReadyToEmit(int $jobsCount, int $lastJobAt): bool
    {
        return ($jobsCount === $this->portion) ||
            ($this->waitTime && (($this->getMoment() - $lastJobAt) > $this->waitTime));
    }

    private function makeList(array $jobDescs): Bulk
    {
        return new Bulk(array_map(function($jobDesc) {
            //TODO: handle JobCreatingException?
            return $this->makeJob($jobDesc);
        }, $jobDescs));
    }

    private function getMoment(): int
    {
        return intval(microtime(true) * 1000);
    }


}
