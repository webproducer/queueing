<?php
namespace Queueing;

use function Amp\asyncCall;

/**
 * Class JobsQueueBulkSubscriber
 * @package Queueing
 * @todo Add max wait time?
 */
class JobsQueueBulkSubscriber extends AbstractJobsQueueSubscriber
{

    private $portion = 1;

    public function setBulkSize(int $size): self
    {
        $this->portion = $size;
        return $this;
    }

    public function subscribe(): Subscription
    {
        asyncCall(function () {
            //TODO: use timeout to cancel Promise returned by reserve() on exiting?
            $jobs[] = [];
            while ($jobData = yield $this->nextJob()) {
                $jobs[] = $jobData;
                if (count($jobs) === $this->portion) {
                    yield $this->emit($this->makeList($jobs));
                    $jobs = [];
                }
            }
            if (!empty($jobs)) {
                yield $this->emit($this->makeList($jobs));
            }
            $this->complete();
        });
        return $this->makeSubscription();
    }

    private function makeList(array $jobDescs): Bulk
    {
        return new Bulk(array_map(function($jobDesc) {
            //TODO: handle JobCreatingException?
            return $this->makeJob($jobDesc);
        }, $jobDescs));
    }


}
