<?php
namespace Queueing;

use function Amp\asyncCall;


class JobsQueueSubscriber extends AbstractJobsQueueSubscriber //implements SubscriberInterface
{

    public function subscribe(): Subscription
    {
        asyncCall(function() {
            //TODO: use timeout to cancel Promise returned by reserve() on exiting?
            while ($jobData = yield $this->nextJob()) {
                yield $this->emit($jobData);
            }
            $this->complete();
        });
        return $this->makeSubscription();
    }

}
