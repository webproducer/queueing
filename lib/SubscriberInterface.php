<?php
namespace Queueing;

interface SubscriberInterface
{

    const TIMED_OUT = 'TIMED_OUT';

    /**
     * @return Subscription
     */
    public function subscribe(): Subscription;

}
