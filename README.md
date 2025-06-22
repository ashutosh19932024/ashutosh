Design the high-level architecture for the system:

Backend (Server):
1.	I am using REST APIs for fetching and sending messages.
2.	For security using JWT for authentication. If user login create Token and using that token send and received API I am using.
3.	Manages WebSocket connections for real-time messaging. For this  I am using channels redis to Implement Web socket.
4.	Stores user information, messages, and metadata.
5.	Using ORM to create table and get data from database.

WebSocket Server:
1.	Manages real-time communication using Django Channels.
2.	Broadcasts messages to specific user groups.
Security Layers:
1.	 JWT for user authentication.
2.	Create middleware to authentication all API using Token.
3.	token expired mechanism and refresh mechanism
Database:
1.	I am Using SQL server Database to store user and message data.
Frontend (Client):
1.	Provides the user interface for chat functionality.
2.	Handles WebSocket connections for real-time communication.
3.	Fetches and displays messages using REST APIs.
4.	Implements retry mechanisms for WebSocket reconnections.
5.	Using Django template to create Frontend UI

Explain the Components with data flow involved, such as APIs, WebSocket servers, databases, and caches.

API Server
Handles HTTP requests for user authentication, user management, and messaging operations.
1.	Signup API → Registers a new user (saves user info in database).
2.	Login API → Authenticates user and generates a token (e.g., JWT) → cache token (e.g., Redis).
3.	Search User API → Searches users (fetches from DB or cache).
4.	Message API → Sends message to selected user (writes to DB + emits to WebSocket server).
5.	Received API → On receiver side, fetches new + historical messages (from DB/cache).
6.	User status API- Get the user is online or not. Here I am checking last activity of user and find user status.
WebSocket:
1.	  Django Channels for WebSocket communication
2.	 Redis as the channel layer (message broker between WebSocket connections)
3.	Received API to help clients pull real-time messages (if not using pure WebSocket client-side listening)
Databases:
1.	Microsoft SQL server
caches.
1.	Caches auth tokens → quick validation on every API call.
Propose a Database schema
1.	User ↔ Message:
a.	from_user and to_user in the Message table reference the User table.
b.	Ensures that messages are tied to valid users.
2.	User ↔ WebSocketToken:
a.	Each user can have multiple WebSocket tokens for authentication.
3.	User ↔ ChatSession:
a.	Tracks active chat sessions between two users.
4.	User ↔ AuditLog:
a.	Logs all actions performed by users for monitoring and security.
Describe how you would implement security
1.	JWT tokens are used to authenticate users and authorize access to APIs. Ensuring the tokens are securely generated and managed is critical.
2.	Middleware ensures that all incoming API requests are authenticated using the JWT token.
Suggest ways to optimize the system for cost without compromising performance or reliability

Category	Optimization
Infrastructure	Use cloud services, containers, and managed databases.
Database	Partition tables, archive old data, and optimize queries.
WebSocket	Use Redis, limit idle connections, and batch messages.
API	Implement caching, rate limiting, and payload compression.
Message Storage	Compress messages and use cold storage for old data.
Authentication	Use short-lived tokens and lightweight middleware.
Logging	Centralize logs and log only essential data.
Deployment	Use CI/CD pipelines and multi-region deployment.

Below the Application Screenshot are given.

 

 

 

 
 



