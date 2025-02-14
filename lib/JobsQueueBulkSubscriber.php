<?php

namespace Queueing;

use Amp\Deferred;
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
            while (!$this->isStopped()) {
                // Fill in the $jobs while a job exists and amount of jobs less than the portion.
                do {
                    $jobData = yield $this->nextJob($this->waitTime);
                    if (is_array($jobData)) {
                        $jobs[] = $jobData;
                    }
                } while (is_array($jobData) && count($jobs) < $this->portion);


                if (count($jobs)) {
                    $resultWasSent = new Deferred();
                    /**
                     * Send job and deferred to AsyncQueueProcessor
                     * @see AsyncQueueProcessor::process()
                     */
                    yield $this->emit([$this->makeList($jobs), $resultWasSent]);
                    // Wait for the result was sent
                    yield $resultWasSent->promise();
                    yield $this->processResults();
                    $jobs = [];
                }

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
