# Пример скрипта Python с ошибкой

def main():
    # Производим какие-то действия
    print("Начало работы скрипта")
    
    # Здесь происходит ошибка
    raise ValueError("Произошла ошибка")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Ошибка:", e)
        exit(2)  # Код возврата 2 указывает на ошибку
