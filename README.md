# Luxor Take Home

TCP Message Processing System with long-lived connetions.

System Goals:

- Listen and handle TCP connections
- Implement the communication protocol as listed below
- Track and maintain state information

## Components

1. TCP Server
2. TCP Client
3. Message Processor (bonus)

## TODO

- [X] Implement Server
- [ ] Implement Client
- [ ] Implement Auth Flow

{"id":30,"method":"authorize","params":{"username": "admin"}}

{"id":1,"method":"job","params":{"job_id":1,"server_nonce":"123"}}

{"id":30,"method":"job","params":{"job_id":1,"server_nonce":"123"}}
{"id":33,"method":"job","params":{"job_id":2,"server_nonce":"987"}}