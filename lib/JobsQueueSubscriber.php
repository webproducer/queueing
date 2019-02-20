<?php

namespace Queueing;

use function Amp\asyncCall;

class JobsQueueSubscriber extends AbstractJobsQueueSubscriber
{
    /**
     * @return Subscription
     */
    public function subscribe(): Subscription
    {
        asyncCall(function () {
            //TODO: use timeout to cancel Promise returned by reserve() on exiting?
            while ($jobData = yield $this->nextJob()) {
                yield $this->emitAndProcess($this->makeJob($jobData));
            }
            $this->complete();
        });
        return $this->makeSubscription();
    }
}
