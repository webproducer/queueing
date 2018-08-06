<?php
namespace Queueing;

/**
 * Class BulkPerformingResult
 * @package Queueing
 * @deprecated - Use PerformingResult class instead
 */
class BulkPerformingResult
{
    private $done = [];
    private $errors = [];

    public function addError(PerformingException $e): void
    {
        $this->errors[] = $e;
    }

    public function setAsDone(JobInterface $job): void
    {
        $this->done[] = $job;
    }

    /**
     * @return PerformingException[]
     */
    public function getErrors()
    {
        return $this->errors;
    }

    /**
     * @return JobInterface[]
     */
    public function getPerformed(): array
    {
        return $this->done;
    }

}
