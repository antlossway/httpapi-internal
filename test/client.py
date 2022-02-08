#curl -X POST "https://<api_key>:<api_secret@localhost:8000/sms" -H "accept: application/json" -H "Content-Type: application/json; charset=utf-8" -d '{"from":"ABC","to":"96650403020","content":"Hello World"}' -i

#curl -X POST "http://localhost:8000/callback_dlr" -H "accept: application/json"  -H "Content-Type: application/json; charset=utf-8" -d '{"msgid": "HTTPmsgid","status":"DELIVERED"}' -i
