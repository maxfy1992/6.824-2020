## mit6.824
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
```