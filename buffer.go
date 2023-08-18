package main

import "io"

type ByteArrayWriter struct {
	buffer *[]byte
}

func NewByteArrayWriter(buffer *[]byte) io.Writer {
	return &ByteArrayWriter{buffer: buffer}
}

func (w *ByteArrayWriter) Write(data []byte) (int, error) {
	*w.buffer = append(*w.buffer, data...)
	return len(data), nil
}

type ByteArrayReader struct {
	data   []byte
	offset int
}

func NewByteArrayReader(data []byte) io.Reader {
	return &ByteArrayReader{data: data}
}

func (r *ByteArrayReader) Read(buffer []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}

	n := copy(buffer, r.data[r.offset:])
	r.offset += n

	return n, nil
}
