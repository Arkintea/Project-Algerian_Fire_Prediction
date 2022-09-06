# Project-Algerian_Fire_Prediction

## Start Marchine Learning Project

https://algerianfireprediction.herokuapp.com/

### Software and Account Requirements

1. [Github Account](https://github.com/)
2. [Heroku Account](https://id.heroku.com/login)
3. [VS Code IDE](https://code.visualstudio.com/download)
4. [GIT cli](https://git-scm.com/downloads)

Creating conda environment
```
conda create -p venv python==3.7 -y
```

Activate conda environment
```
conda activate venv/
```
OR
```
conda activate venv
```

Install requirements
```
pip install -r requirements.txt
```

To add file to git
```
git add <filename>
```
OR
```
git add .
```

```
Note: To ignore file or folder from git we can write the name of the file/folder in .gitignore folder
```

To check status
```
git status
```

To check all versions maintained by git
```
git log
```

To create version/commit all changes by git
```
git commit -m "message"
```

To send version/changes to github
```
git push origin main
```

To check remote url
```
git remote -v
```

## Deployment
To setup CI/CD pipeline in heroku we need 3 information
```
HEROKU_EMAIL = arkintea@gmail.com
HEROKU_API_KEY = <>
HEROKU_APP_NAME = 
```

## BUILD DOCKER IMAGE
```
docker build -t <image_name>:<tagname> .
```
Note: Image name for docker must be lowercase
```

To list docker image
```
docker images
```

Run docker image
```
docker run -p 5000:5000 -e PORT=5000 f8c749e73678
```

To check running container in docker
```
docker ps
```

To stop docker conatiner
```
docker stop <container_id>
```


To install ipykernel
```
install ipykernel
```
