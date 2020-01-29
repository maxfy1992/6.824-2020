## mit6.824 2020
[schedule](https://pdos.csail.mit.edu/6.824/schedule.html)

## lab1
[lab1](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

```
install go 1.13.1
go version go1.13.1 darwin/amd64

git clone git://g.csail.mit.edu/6.824-golabs-2020 6.824

build erros then

go get github.com/maxfy1992/6.824-2020
cd src/mrapps
go build -buildmode=plugin wc.go
go run mrsequential.go ../mrapps/wc.so pg*.txt
more mr-out-0

// go test commit lab1 go test 
./mr
go test
 PASS
 ok      github.com/maxfy1992/6.824-2020/src/mr  133.425s

// go test commit lab1 test-mr.sh 
sh test-mr.sh
--- wc test: PASS
--- indexer test: PASS
--- map parallelism test: PASS
--- reduce parallelism test: PASS
--- crash test: PASS
*** PASSED ALL TESTS
```

### 遗留问题

- server如何优雅通过rpc关闭worker，目前虽然可以关闭但是server端会有错误EOF错误
- go func 闭包，如何显式设置哪些变量需要copy，目前schedule处task存在竞争
- test-mr.sh中修改了部分测试代码，这部分可以通过worker启动获取pid来避免指定一个worker unix socket