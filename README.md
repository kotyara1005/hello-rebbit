# Domain resolve storage service

## Sender

It gets domains from DB and sends them to RabbitMQ.

## Consumer

Gets domains from RabbitMQ, resolves and commits to DB.

## Installation

Clone it

## Init DB

run db.py
