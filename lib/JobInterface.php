<?php
namespace Queueing;

interface JobInterface
{
    /**
     * @return string
     */
    function getPayload(): string;

    /**
     * @return int
     */
    function getId(): int;

}
