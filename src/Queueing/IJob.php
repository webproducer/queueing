<?php
namespace Queueing;

interface IJob
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
