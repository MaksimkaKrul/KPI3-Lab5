package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/MaksimkaKrul/KPI3-Lab5/datastore"
)

func main() {
	// 1. Тест создания БД и записи данных
	db, err := datastore.Open("testdb")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Уменьшаем maxSize для теста (1KB вместо 10MB)
	db.maxSize = 1024

	fmt.Println("Записываем данные...")
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d_%s", i, strings.Repeat("x", 500)) // Большие значения
		if err := db.Put(key, value); err != nil {
			panic(err)
		}
		fmt.Printf("Записано: %s\n", key)
	}

	// 2. Тест чтения
	fmt.Println("\nЧитаем данные...")
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		value, err := db.Get(key)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Прочитано: %s -> %s (первые 20 символов)\n", key, value[:20])
	}

	// 3. Проверка сегментов
	fmt.Println("\nПроверяем сегменты...")
	files, _ := os.ReadDir("testdb")
	fmt.Printf("Найдено файлов: %d\n", len(files))
	for _, f := range files {
		fmt.Println(f.Name())
	}
}
