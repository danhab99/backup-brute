package main

import (
	"bytes"
	"io"
	"sync"
)

func chanWorker[InutType any, OutputType any](inputChan chan InutType, workerCount int, processTask func(in InutType) OutputType) chan OutputType {
	outputChan := make(chan OutputType)
	var wg sync.WaitGroup
	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for in := range inputChan {
				outputChan <- processTask(in)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(outputChan)
	}()

	return outputChan
}

func syncronizeBuffers(in chan IndexedBuffer) io.ReadCloser {
	reader, writer := io.Pipe()
	go func() {
		buffMap := make(map[int]*bytes.Buffer)
		count := 0

		for buff := range in {
			buffMap[buff.i] = buff.buffer

			for buf, ok := buffMap[count]; ok; {
				check(io.Copy(writer, buf))
				delete(buffMap, count)
				count++
			}
		}
		check0(writer.Close())
	}()
	return reader
}

func NewBufferPool() sync.Pool {
	return sync.Pool{
		New: func() interface{} { return new(bytes.Buffer) },
	}
}

func makeChunks(in io.ReadCloser, chunkSize int64) (out chan IndexedBuffer) {
	out = make(chan IndexedBuffer)

	var bufferPool = NewBufferPool()

	go func() {
		i := 0
		for {
			chunk := bufferPool.Get().(*bytes.Buffer)
			n, err := io.CopyN(chunk, in, chunkSize)
			out <- IndexedBuffer{
				pool:   &bufferPool,
				buffer: chunk,
				i:      i,
			}
			i++

			if n > chunkSize || err != nil {
				close(out)
				break
			}
		}
	}()
	return
}
