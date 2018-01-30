<?php
namespace Queueing;


interface BulkJobPerformerInterface
{

    /**
     * @param JobInterface[] $jobs
     * @return BulkPerformingResult
     * @throws BulkPerformingException
     */
    public function bulkPerform(array $jobs): BulkPerformingResult;

}
