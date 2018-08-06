<?php
namespace Queueing;


interface PerformingResultInterface
{
    /**
     * @return array|JobInterface[]
     */
    public function getDoneJobs(): array;

    /**
     * @return array|PerformingException[]
     */
    public function getErrors(): array;

}
