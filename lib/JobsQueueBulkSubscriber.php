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
            while ($jobData = yield $this->nextJob($this->waitTime)) {
                if ($jobData !== self::TIMED_OUT) {
                    $jobs[] = $jobData;
                }
                $jobsCnt = count($jobs);
                if ($jobsCnt && (($jobsCnt === $this->portion) || ($jobData === self::TIMED_OUT))) {
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

    private function makeList(array $jobDescs): Bulk
    {
        return new Bulk(array_map(function ($jobDesc) {
            //TODO: handle JobCreatingException?
            return $this->makeJob($jobDesc);
        }, $jobDescs));
    }
}
