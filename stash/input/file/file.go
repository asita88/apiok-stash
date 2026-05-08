package file

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/kevwan/go-stash/stash/config"
	"github.com/nxadm/tail"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/threading"
)

const timestampFormat = "2006-01-02T15:04:05.000Z"

type ConsumeHandler interface {
	Consume(key, val string) error
}

type FileInput struct {
	conf    config.FileConf
	handler ConsumeHandler
	tails   []*tail.Tail
	quit    chan struct{}
	wg      sync.WaitGroup
}

func NewFileInput(conf config.FileConf, handler ConsumeHandler) (*FileInput, error) {
	paths, err := expandPaths(conf.Paths)
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("no files match paths: %v", conf.Paths)
	}

	cfg := tail.Config{
		Follow:        conf.Follow,
		ReOpen:        true,
		MustExist:     false,
		Poll:          true,
		MaxLineSize:   conf.MaxLineSize,
		CompleteLines: true,
		Logger:        tail.DiscardingLogger,
	}

	if !conf.FromBeginning {
		cfg.Location = &tail.SeekInfo{Offset: 0, Whence: 2}
	}

	tails := make([]*tail.Tail, 0, len(paths))
	for _, path := range paths {
		t, err := tail.TailFile(path, cfg)
		if err != nil {
			for _, tt := range tails {
				_ = tt.Stop()
			}
			return nil, err
		}
		tails = append(tails, t)
	}

	return &FileInput{
		conf:    conf,
		handler: handler,
		tails:   tails,
		quit:    make(chan struct{}),
	}, nil
}

func (f *FileInput) Start() {
	for _, t := range f.tails {
		tail := t
		f.wg.Add(1)
		threading.GoSafe(func() {
			defer f.wg.Done()
			f.readLines(tail)
		})
	}
	f.wg.Wait()
}

func (f *FileInput) Stop() {
	close(f.quit)
	for _, t := range f.tails {
		_ = t.Stop()
	}
	f.wg.Wait()
}

func (f *FileInput) readLines(t *tail.Tail) {
	for {
		select {
		case <-f.quit:
			return
		case line, ok := <-t.Lines:
			if !ok {
				return
			}
			if line.Err != nil {
				logx.Errorf("tail %s: %v", t.Filename, line.Err)
				continue
			}

			val := strings.TrimSpace(line.Text)
			if val == "" {
				continue
			}

			if f.conf.Format == "plain" {
				val = f.wrapPlain(val, t.Filename)
			}

			if err := f.handler.Consume("", val); err != nil {
				logx.Errorf("consume %s: %v", t.Filename, err)
			}
		}
	}
}

func (f *FileInput) wrapPlain(line, source string) string {
	m := map[string]interface{}{
		"message":    line,
		"@timestamp": time.Now().UTC().Format(timestampFormat),
		"source":     source,
	}
	bs, _ := json.Marshal(m)
	return string(bs)
}

func expandPaths(paths []string) ([]string, error) {
	var result []string
	seen := make(map[string]struct{})

	for _, p := range paths {
		matches, err := filepath.Glob(p)
		if err != nil {
			return nil, err
		}
		for _, m := range matches {
			if _, ok := seen[m]; ok {
				continue
			}
			seen[m] = struct{}{}
			result = append(result, m)
		}
	}

	return result, nil
}
