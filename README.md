# mysql-issue
A test case for MySQL 8.0.29

Uses testcontainers and requires docker installed.
```
./gradlew clean test
```

The test is the same but it will run in 4 different configurations
1. With MySQL 8.0.28 and the client cache enabled 
2. With MySQL 8.0.28 and the client cache disabled 
3. With MySQL 8.0.29 and the client cache enabled (it fails)
4. With MySQL 8.0.29 and the client cache disabled 

The failure happens with case 3.
