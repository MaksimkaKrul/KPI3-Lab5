package datastore

import (
	"fmt"
	"strings"
	"testing"
)

func TestDb(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	pairs := [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k2", "v2.1"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("file growth", func(t *testing.T) {
		sizeBefore, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		sizeAfter, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		if sizeAfter <= sizeBefore {
			t.Errorf("Size does not grow after put (before %d, after %d)", sizeBefore, sizeAfter)
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(tmp)
		if err != nil {
			t.Fatal(err)
		}

		uniquePairs := make(map[string]string)
		for _, pair := range pairs {
			uniquePairs[pair[0]] = pair[1]
		}

		for key, expectedValue := range uniquePairs {
			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if value != expectedValue {
				t.Errorf("Get(%q) = %q, wanted %q", key, value, expectedValue)
			}
		}
	})
}
func TestSegmentCreation(t *testing.T) {
	tmp := t.TempDir()

	// 1. Создаем БД с маленьким maxSize
	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	db.maxSize = 1024 // 1KB для теста

	// 2. Записываем данные до превышения maxSize
	largeValue := strings.Repeat("x", 800) // Большое значение
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		if err := db.Put(key, largeValue); err != nil {
			t.Fatal(err)
		}
	}

	// 3. Проверяем, что создалось несколько сегментов
	if len(db.segments) < 2 {
		t.Errorf("Ожидалось создание нескольких сегментов, получили %d", len(db.segments))
	}
}

func TestCrossSegmentRead(t *testing.T) {
	tmp := t.TempDir()

	// 1. Создаем и заполняем БД
	db1, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	db1.maxSize = 1024 // 1KB для теста

	// Записываем данные, которые точно вызовут ротацию
	keys := []string{"k1", "k2", "k3", "k4"}
	for _, key := range keys {
		largeValue := strings.Repeat(key, 500) // Большие значения
		if err := db1.Put(key, largeValue); err != nil {
			t.Fatal(err)
		}
	}
	db1.Close()

	// 2. Открываем заново и проверяем чтение
	db2, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	for _, key := range keys {
		val, err := db2.Get(key)
		if err != nil {
			t.Errorf("Ошибка чтения ключа %s: %v", key, err)
		}
		if !strings.HasPrefix(val, key) {
			t.Errorf("Некорректное значение для ключа %s: %s", key, val[:20])
		}
	}
}
