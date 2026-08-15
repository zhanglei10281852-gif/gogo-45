package codec

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const DefaultMaxDocumentBytes int64 = 16 << 20

func Decode(reader io.Reader, target any) error {
	return DecodeLimit(reader, target, DefaultMaxDocumentBytes)
}

func DecodeLimit(reader io.Reader, target any, limit int64) error {
	if reader == nil {
		return errors.New("reader is nil")
	}
	if target == nil {
		return errors.New("decode target is nil")
	}
	if limit < 1 {
		return errors.New("limit must be positive")
	}
	limited := &io.LimitedReader{R: reader, N: limit + 1}
	data, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read JSON: %w", err)
	}
	if int64(len(data)) > limit {
		return fmt.Errorf("JSON document exceeds %d bytes", limit)
	}
	return DecodeBytes(data, target)
}

func DecodeBytes(data []byte, target any) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return errors.New("JSON document is empty")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return classify(err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("JSON document contains multiple values")
		}
		return classify(err)
	}
	return nil
}
func classify(err error) error {
	var syntax *json.SyntaxError
	if errors.As(err, &syntax) {
		return fmt.Errorf("invalid JSON at byte %d: %w", syntax.Offset, err)
	}
	var mismatch *json.UnmarshalTypeError
	if errors.As(err, &mismatch) {
		field := mismatch.Field
		if field == "" {
			field = "<root>"
		}
		return fmt.Errorf("wrong JSON type for %s at byte %d: expected %s", field, mismatch.Offset, mismatch.Type)
	}
	return err
}

type StreamDecoder struct {
	scanner *bufio.Scanner
	line    int
	maxLine int
}

func NewStreamDecoder(reader io.Reader, maxLineBytes int) (*StreamDecoder, error) {
	if reader == nil {
		return nil, errors.New("reader is nil")
	}
	if maxLineBytes < 1 {
		return nil, errors.New("max line bytes must be positive")
	}
	scanner := bufio.NewScanner(reader)
	initial := 64 * 1024
	if maxLineBytes < initial {
		initial = maxLineBytes
	}
	scanner.Buffer(make([]byte, initial), maxLineBytes)
	return &StreamDecoder{scanner: scanner, maxLine: maxLineBytes}, nil
}

func (d *StreamDecoder) Next(target any) (bool, error) {
	for d.scanner.Scan() {
		d.line++
		data := bytes.TrimSpace(d.scanner.Bytes())
		if len(data) == 0 {
			continue
		}
		if err := DecodeBytes(data, target); err != nil {
			return false, fmt.Errorf("line %d: %w", d.line, err)
		}
		return true, nil
	}
	if err := d.scanner.Err(); err != nil {
		return false, fmt.Errorf("read line %d (limit %d bytes): %w", d.line+1, d.maxLine, err)
	}
	return false, nil
}

type Object struct {
	fields map[string]json.RawMessage
}

func ParseObject(data []byte, allowed ...string) (Object, error) {
	var fields map[string]json.RawMessage
	if err := DecodeBytes(data, &fields); err != nil {
		return Object{}, err
	}
	if fields == nil {
		return Object{}, errors.New("expected JSON object")
	}
	allow := make(map[string]bool, len(allowed))
	for _, name := range allowed {
		allow[name] = true
	}
	for name := range fields {
		if !allow[name] {
			return Object{}, fmt.Errorf("unknown field %q", name)
		}
	}
	return Object{fields: fields}, nil
}

func (o Object) Required(name string, target any) error {
	value, ok := o.fields[name]
	if !ok {
		return fmt.Errorf("required field %q is missing", name)
	}
	if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return fmt.Errorf("required field %q is null", name)
	}
	if err := DecodeBytes(value, target); err != nil {
		return fmt.Errorf("field %q: %w", name, err)
	}
	return nil
}

func (o Object) Optional(name string, target any) (bool, error) {
	value, ok := o.fields[name]
	if !ok {
		return false, nil
	}
	if err := DecodeBytes(value, target); err != nil {
		return true, fmt.Errorf("field %q: %w", name, err)
	}
	return true, nil
}

func Encode(writer io.Writer, value any, pretty bool) error {
	if writer == nil {
		return errors.New("writer is nil")
	}
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	if pretty {
		encoder.SetIndent("", "  ")
	}
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("encode JSON: %w", err)
	}
	return nil
}
