<?php
namespace Queueing;

use Amp\Promise;


interface BulkJobPerformerInterface extends JobPerformerInterface
{

    /**
     * @param Bulk $jobs
     * @return PerformingResult|Promise
     */
    public function bulkPerform(Bulk $jobs);

}
