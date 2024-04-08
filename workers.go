package main

import (
	"bytes"
	"io"
	"sync"

	"mkm.pub/syncpool"
)

func chanWorker[InutType any, OutputType any](inputChan chan InutType, workerCount int, processTask func(in InutType) OutputType) chan OutputType {
	outputChan := make(chan OutputType)
	var wg sync.WaitGroup
	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for in := range inputChan {
				// runtime.GC()
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

func pipeWorker(input io.ReadCloser, chunkSize int64, workerCount int, processChunk func(out io.Writer, in io.Reader)) io.ReadCloser {
	pool := NewBufferPool()
	chunkChan := makeChunks(input, &pool, chunkSize)

	out := chanWorker[IndexedBuffer, IndexedBuffer](chunkChan, workerCount, func(in IndexedBuffer) IndexedBuffer {
		r := pool.Get()
		processChunk(in.buffer, r)
		pool.Put(in.buffer)

		return IndexedBuffer{
			i:      in.i,
			buffer: r,
		}
	})

	return syncronizeBuffers(out, &pool)
}

type BufferPool = syncpool.Pool[*BufferType]

func NewBufferPool() BufferPool {
	return syncpool.New[*BufferType](func() *BufferType {
		return new(BufferType)
	})
}

func syncronizeBuffers(in chan IndexedBuffer, pool *BufferPool) io.ReadCloser {
	reader, writer := io.Pipe()
	go func() {
		buffMap := make(map[int]*bytes.Buffer)
		count := 0

		for buff := range in {
			buffMap[buff.i] = buff.buffer

			for buf, ok := buffMap[count]; ok; {
				check(io.Copy(writer, buf))
				pool.Put(buf)
				delete(buffMap, count)
				count++
			}
		}
		check0(writer.Close())
	}()
	return reader
}

func makeChunks(in io.ReadCloser, bufferPool *BufferPool, chunkSize int64) (out chan IndexedBuffer) {
	out = make(chan IndexedBuffer)

	go func() {
		i := 0
		for {
			chunk := bufferPool.Get()
			n, err := io.CopyN(chunk, in, chunkSize)
			out <- IndexedBuffer{
				buffer: chunk,
				i:      i,
			}
			i++

			if n < chunkSize || err != nil {
				close(out)
				break
			}
		}
	}()
	return
}
