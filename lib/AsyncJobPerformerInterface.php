<?php
namespace Queueing;

use Amp\Promise;

interface AsyncJobPerformerInterface
{

    public function perform(JobInterface $job): Promise;

}
