## Run kafka with Docker
* get license here : https://lenses.io/start/
* run kafka :
```shell
docker run -d -e ADV_HOST=127.0.0.1 -e EULA="<licence_url>" -p 3030:3030 -p 9092:9092 --name lensesio-box lensesio/box
```
* run Stream.scala