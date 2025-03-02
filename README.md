# About this code

Submission for the MongoDB database manipulation purpoused by ViveLibre.

## Important

To execute this class is mandatory to execute this cli command:

```bash
docker/podman run --detach --replace --name data_db -p 3000:27017 -v database_vol:/data/db/ docker.io/mongodb/mongodb-community-server:latest 
```
And to populate the Mongo Database with fresh new data with the provided books.json file.

I used MongoDB Compass as DB manager, but could be done with any DB manager of your choice.
