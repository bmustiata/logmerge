#!/usr/bin/env bash

go build logmerge.go
GOOS=windows GOARCH=amd64 go build logmerge.go
