package datastore

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const outFileName = "current-data"

var ErrNotFound = fmt.Errorf("record does not exist")

type hashIndex map[string]int64

type Db struct {
	out       *os.File
	outOffset int64
	outPath   string // Добавляем путь к текущему файлу
	dir       string // Добавляем путь к директории БД
	index     hashIndex
	segments  []string // Пути к файлам сегментов
	maxSize   int64    // Максимальный размер сегмента
}

func Open(dir string) (*Db, error) {
	outputPath := filepath.Join(dir, outFileName)
	f, err := os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}

	db := &Db{
		out:      f,
		outPath:  outputPath,
		dir:      dir, // Сохраняем путь к директории
		index:    make(hashIndex),
		segments: []string{outputPath}, // Начинаем с одного сегмента
		maxSize:  10 * 1024 * 1024,     // 10MB по умолчанию
	}

	err = db.recover()
	if err != nil && err != io.EOF {
		return nil, err
	}
	return db, nil
}

func (db *Db) recover() error {
	f, err := os.Open(db.out.Name())
	if err != nil {
		return err
	}
	defer f.Close()

	in := bufio.NewReader(f)
	for err == nil {
		var (
			record entry
			n      int
		)
		n, err = record.DecodeFromReader(in)
		if errors.Is(err, io.EOF) {
			if n != 0 {
				return fmt.Errorf("corrupted file")
			}
			break
		}

		db.index[record.key] = db.outOffset
		db.outOffset += int64(n)
	}
	return err
}

func (db *Db) Close() error {
	return db.out.Close()
}

func (db *Db) Get(key string) (string, error) {
	position, ok := db.index[key]
	if !ok {
		return "", ErrNotFound
	}

	file, err := os.Open(db.out.Name())
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(position, 0)
	if err != nil {
		return "", err
	}

	var record entry
	if _, err = record.DecodeFromReader(bufio.NewReader(file)); err != nil {
		return "", err
	}
	return record.value, nil
}

func (db *Db) Put(key, value string) error {
	e := entry{
		key:   key,
		value: value,
	}
	n, err := db.out.Write(e.Encode())
	if size, _ := db.Size(); size >= db.maxSize {
		if err := db.rotateSegment(); err != nil {
			return err
		}
	}
	if err == nil {
		db.index[key] = db.outOffset
		db.outOffset += int64(n)
	}
	return err
}

func (db *Db) Size() (int64, error) {
	info, err := db.out.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (db *Db) rotateSegment() error {
	fmt.Printf("Rotating segment in dir: %s\n", db.dir) // Отладочный вывод
	// 1. Закрываем текущий файл
	if err := db.out.Close(); err != nil {
		return err
	}

	// 2. Переименовываем текущий файл (используем db.outPath)
	newName := fmt.Sprintf("segment-%d.dat", time.Now().UnixNano())
	newPath := filepath.Join(db.dir, newName) // Используем db.dir
	if err := os.Rename(db.outPath, newPath); err != nil {
		return err
	}

	// 3. Добавляем в список сегментов
	db.segments = append(db.segments, newPath)

	// 4. Создаем новый файл (используем db.outPath)
	f, err := os.OpenFile(db.outPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}

	db.out = f
	db.outOffset = 0
	db.index = make(hashIndex)

	return nil
}
