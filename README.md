## Автоматизированные тесты для PostgreSQL

##   Настройка перед использованием

 1. Конфигурационный файл .env

  Создайте файл `.env` в корне проекта со следующей структурой:
      
      
      DB_HOST=host
      
      DB_PORT=5432
      
      DB_NAME=test_BD
      
      DB_USER=login
      
      DB_PASSWORD=password
      
      DB_DEFAULT_TABLE=name_table

      
     
    

1. В базе данных должны быть настроены права для пользователя. Таблица People должна быть создана

2. Требования к таблице People:

  3 столбца с разными типами данных (порядковый Index, FirstName, LastName, DataOfBirth\):

         •	SERIAL для Index:

         •	VARCHAR(255) для FirstName и LastName:

         •	FirstName и LastName NOT NULL

         •	DATE для DataOfBirth

### Запуск тестов

 1. Перед запуском тестов необходимо настроить виртуальное окружение

      a. перейти в корень проекта
  
      b. (windows) .venv\Scripts\activate или (bash) source .venv/bin/activate

 2. Тесты запускаются из командной строки из корня проекта:

      a. (cmd) set ENV_FILE=.env.windows && python -m unittest discover -v -s tests
  
      b. (powershell) $env:ENV_FILE=".env.windows"; python -m unittest discover -v -s tests
  
      с. (bash) ENV_FILE=.env.windows python3 -m unittest discover -v -s tests
  
      d. (bash) export ENV_FILE=.env.windows python3 -m unittest discover -v -s tests




