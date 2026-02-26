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

## Testing:

**Auth**

{"id":30,"method":"authorize","params":{"username": "admin"}}
{"id":30,"method":"authorize","params":{"username": "user"}}

**Submit**

```
echo -n "<server_nonce><client_nonce>" | sha256sum
```

echo -n "e4c6cc43dcfd249975542dfc49c62cab123" | sha256sum

{"id":null,"method":"job","params":{"job_id":43,"server_nonce":"e4c6cc43dcfd249975542dfc49c62cab"}}
{"id":null,"method":"job","params":{"job_id":43,"server_nonce":"02d2254e01cc607c526137e24af40639"}}

{"id":2,"method":"submit","params":{"job_id":43,"client_nonce":"123","result":"742ca7388a21d05b49ca1c2ea330b2d4507cb2dc6ce5bce84757c0b3ffc925ec"}}