package logger

import (
	"encoding/xml"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	LOGGER_FATAL = 1
	LOGGER_ERROR = 10
	LOGGER_WARN  = 100
	LOGGER_INFO  = 1000
	LOGGER_DEBUG = 10000
)

const (
//LOCK_FILE_ROOT = "/tmp"
)

type ILogger interface {
	Load(conf_path string) error
	Reload()
	LogAccDebug(strFormat string, args ...interface{})
	LogAccInfo(strFormat string, args ...interface{})
	LogAccWarn(strFormat string, args ...interface{})
	LogAccError(strFormat string, args ...interface{})
	LogAccFatal(strFormat string, args ...interface{})

	LogAppDebug(strFormat string, args ...interface{})
	LogAppInfo(strFormat string, args ...interface{})
	LogAppWarn(strFormat string, args ...interface{})
	LogAppError(strFormat string, args ...interface{})
	LogAppFatal(strFormat string, args ...interface{})

	LogSysDebug(strFormat string, args ...interface{})
	LogSysInfo(strFormat string, args ...interface{})
	LogSysWarn(strFormat string, args ...interface{})
	LogSysError(strFormat string, args ...interface{})
	LogSysFatal(strFormat string, args ...interface{})

	Close()
}

const (
	APP = 0
	ACC = 1
	SYS = 2
)

const (
	file_logger_nums = 3
)

var g_single_logger *logger = nil

type logger struct {
	ptrLogArray [file_logger_nums]*filelogger
	LogConfPath string
}

type filelogger_config struct {
	FilePath     string
	nLoglevel    int
	nMaxFileSize int
	nRollupFiles int
	bFlush       bool
}

const (
	SUCCEED             = 0
	XML_FILE_NOT_FIND   = 1
	XML_PARSED_FAILED   = 2
	XML_TAG_NOT_MATCHED = 3
)

type logErr struct {
	nErrCode int
}

func (this *logErr) setErr(nNewErr int) {
	this.nErrCode = nNewErr
}

func (this *logErr) Error() (strErr string) {
	mapConf := map[int]string{
		SUCCEED:             "Succeed!",
		XML_PARSED_FAILED:   "Xml File Parsed Failed!Maybe Format Error!",
		XML_TAG_NOT_MATCHED: "Xml File Has No Right Tag!",
		XML_FILE_NOT_FIND:   "Xml File Not Find!",
	}

	strErr, Ok := mapConf[this.nErrCode]
	if !Ok {
		strErr = "Unkown Err!"
	}

	return strErr
}

func reflectLogLevel(strLogv string) int {
	strLogv = strings.ToUpper(strLogv)
	loglev_map := map[string]int{
		"DEBUG": 10000,
		"INFO":  1000,
		"WARN":  100,
		"ERROR": 10,
		"FATAL": 1,
	}
	value, ok := loglev_map[strLogv]
	if !ok {
		value = 10000 // 默认10000
	}
	return value
}

func getTagData(strTagName string, strXml string) *filelogger_config {

	IReader := strings.NewReader(strXml)
	Decoder := xml.NewDecoder(IReader)
	var TagName string
	var Conf filelogger_config
	var Token xml.Token
	var Err error

	for Token, Err = Decoder.Token(); Err == nil; Token, Err = Decoder.Token() {

		switch Type := Token.(type) {
		case xml.StartElement:

			// 获取标签名字
			nIndex := strings.Index(strTagName, "/")
			if nIndex == -1 {
				TagName = strTagName[0:]
			} else {
				TagName = strTagName[0:nIndex]
			}

			if TagName == Type.Name.Local {
				if nIndex == -1 { // 找到对应TagName
					var nValue int

					for _, Attr := range Type.Attr {
						if Attr.Name.Local == "loglevel" {
							Conf.nLoglevel = reflectLogLevel(Attr.Value)

						} else if Attr.Name.Local == "filesize" {
							// 确定单位(例如K,M,G)
							mult_nums := 1

							nValue, Err = strconv.Atoi(Attr.Value[:len(Attr.Value)-1])
							if Attr.Value[len(Attr.Value)-1] == 'K' {
								mult_nums *= 1024
							} else if Attr.Value[len(Attr.Value)-1] == 'M' {
								mult_nums *= (1024 * 1024)

							} else if Attr.Value[len(Attr.Value)-1] == 'G' {
								mult_nums *= (1024 * 1024 * 1024)
							} else {
								nValue, Err = strconv.Atoi(Attr.Value)
							}

							if Err == nil {
								Conf.nMaxFileSize = int(nValue)
								Conf.nMaxFileSize *= int(mult_nums)
							}
						} else if Attr.Name.Local == "rollupnums" {
							nValue, Err = strconv.Atoi(Attr.Value)
							if Err == nil {
								Conf.nRollupFiles = int(nValue)
							}
						}
					}

					// 读取内容
					Token, Err = Decoder.Token()
					if Err == nil {
						switch T := Token.(type) {
						case xml.CharData:
							Conf.FilePath = string([]byte(T))
							break
						default:
						}
					}

					return &Conf
				} else {
					strTagName = strTagName[nIndex+1:]
				}
			}

			break
		}
	}

	return nil
}

func Instance() ILogger {
	if g_single_logger == nil {
		g_single_logger = new(logger)
	}
	return g_single_logger
}

func logFormatPrex(strLogLevel string, strFileId string) string {

	Year, Month, Day := time.Now().Date()
	Hour := time.Now().Hour()
	Min := time.Now().Minute()
	Second := time.Now().Second()
	Nano := time.Now().Nanosecond()

	//1972-12-04 13:02:35.233|日志等级|ACC|协程号|文件名字：行号|函数名字| 内容
	strLogPrex := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%03d",
		Year,
		Month,
		Day,
		Hour,
		Min,
		Second,
		Nano/1000000)

	// 获取调用者信息
	Pc, FileName, Line, _ := runtime.Caller(2)
	FileName = func(FileName string) string { // 过滤掉前缀
		var strFilter string
		var nIndex int
		nLen := len(FileName)

		for nIndex = nLen - 1; nIndex >= 0; nIndex-- {
			if FileName[nIndex] == '/' {
				break
			}
		}

		strFilter = FileName[nIndex+1:]
		return strFilter
	}(FileName)

	strFuncName := runtime.FuncForPC(Pc).Name()

	// 获取携程ID(暂时无法获取)

	strLogPrex += "|"
	strLogPrex += strLogLevel
	strLogPrex += "|"
	strLogPrex += strFileId
	strLogPrex += "|"
	strLogPrex += FileName
	strLogPrex += ":"
	strLogPrex += strconv.FormatInt(int64(Line), 10)
	strLogPrex += "|"
	strLogPrex += strFuncName
	strLogPrex += "()"

	return strLogPrex
}

func (this *logger) Load(conf_path string) error {
	var XmlBytes []byte
	var nRet int

	mapFileConf := make(map[string]*filelogger_config, 10) // 创建cap为10的map
	strMap := map[int]string{
		APP: "APP",
		ACC: "ACC",
		SYS: "SYS",
	}

	// 初始化各个数据成员
	// 读取配置文件
	ptrFile, Err := os.Open(conf_path)
	if Err != nil {
		var LogErr logErr
		LogErr.setErr(XML_FILE_NOT_FIND)
		return &LogErr
	}

	defer ptrFile.Close()
	XmlBytes = make([]byte, 1024)
	nRet, _ = ptrFile.Read(XmlBytes)

	strXml := string(XmlBytes[:nRet])
	for _, v := range strMap {
		strTagName := "conf/"
		strTagName += v
		ptrConf := getTagData(strTagName, strXml)
		if ptrConf == nil {
			var LogErr logErr
			LogErr.setErr(XML_PARSED_FAILED)
			return &LogErr
		}
		mapFileConf[v] = ptrConf
	}

	for i := 0; i < file_logger_nums; i++ {
		Name, _ := strMap[i]
		ptrConf, _ := mapFileConf[Name]
		this.ptrLogArray[i], Err = createFilelogger(ptrConf.FilePath, ptrConf.nLoglevel, ptrConf.nMaxFileSize, ptrConf.nRollupFiles)
		if Err != nil {
			// 释放以前打开的句柄
			for _, v := range this.ptrLogArray {
				if v != nil {
					v.Destory()
				}
			}
			return Err
		}
	}

	return nil
}

func (*logger) Reload() {

}

func (this *logger) LogAccDebug(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("DEBUG", "ACC")
	this.ptrLogArray[ACC].LogData(LOGGER_DEBUG, strPrex, strFormat, args...)
}

func (this *logger) LogAccWarn(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("WARN", "ACC")
	this.ptrLogArray[ACC].LogData(LOGGER_WARN, strPrex, strFormat, args...)
}

func (this *logger) LogAccInfo(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("INFO", "ACC")
	this.ptrLogArray[ACC].LogData(LOGGER_INFO, strPrex, strFormat, args...)
}

func (this *logger) LogAccError(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("INFO", "ACC")
	this.ptrLogArray[ACC].LogData(LOGGER_ERROR, strPrex, strFormat, args...)
}

func (this *logger) LogAccFatal(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("FATAL", "ACC")
	this.ptrLogArray[APP].LogData(LOGGER_FATAL, strPrex, strFormat, args...)
}

func (this *logger) LogAppDebug(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("DEBUG", "APP")
	this.ptrLogArray[APP].LogData(LOGGER_DEBUG, strPrex, strFormat, args...)
}

func (this *logger) LogAppInfo(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("INFO", "APP")
	this.ptrLogArray[APP].LogData(LOGGER_INFO, strPrex, strFormat, args...)
}

func (this *logger) LogAppWarn(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("WARN", "APP")
	this.ptrLogArray[APP].LogData(LOGGER_WARN, strPrex, strFormat, args...)
}

func (this *logger) LogAppError(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("ERROR", "APP")
	this.ptrLogArray[APP].LogData(LOGGER_ERROR, strPrex, strFormat, args...)
}

func (this *logger) LogAppFatal(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("FATAL", "APP")
	this.ptrLogArray[APP].LogData(LOGGER_FATAL, strPrex, strFormat, args...)
}

func (this *logger) LogSysDebug(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("DEBUG", "SYS")
	this.ptrLogArray[SYS].LogData(LOGGER_DEBUG, strPrex, strFormat, args...)
}

func (this *logger) LogSysInfo(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("INFO", "SYS")
	this.ptrLogArray[SYS].LogData(LOGGER_DEBUG, strPrex, strFormat, args...)
}

func (this *logger) LogSysWarn(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("WARN", "SYS")
	this.ptrLogArray[SYS].LogData(LOGGER_DEBUG, strPrex, strFormat, args...)
}

func (this *logger) LogSysError(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("ERROR", "SYS")
	this.ptrLogArray[SYS].LogData(LOGGER_DEBUG, strPrex, strFormat, args...)
}

func (this *logger) LogSysFatal(strFormat string, args ...interface{}) {
	strPrex := logFormatPrex("FATAL", "SYS")
	this.ptrLogArray[SYS].LogData(LOGGER_DEBUG, strPrex, strFormat, args...)
}

func (this *logger) Close() {
	for _, FileLogger := range this.ptrLogArray {
		FileLogger.Destory()
	}
}

const (
	LOG_TYPE    = 0
	EXIT_TYPE   = 1
	REFESH_TYPE = 2
)

type logtask struct {
	LogData   []byte
	nLogLevel int
	nLogType  int
}

type filelogger struct {
	file           *os.File
	file_lock      *os.File
	strFileDir     string
	strFileName    string
	nMaxRollupNums int
	nMaxFileSize   int
	nLogLevel      int
	LogChannel     chan *logtask
	StopChannel    chan int
}

func (this *filelogger) dupFile() {

	// 查看当前文件数量
	file_nums := this.checkLogFiles()
	for i := file_nums; i > 0; i-- {
		new_file_path := fmt.Sprintf("%s/%s.%d", this.strFileDir, this.strFileName, i)
		old_file_path := fmt.Sprintf("%s/%s.%d", this.strFileDir, this.strFileName, i-1)
		if i == 1 {
			old_file_path = fmt.Sprintf("%s/%s", this.strFileDir, this.strFileName)
		}
		os.Rename(old_file_path, new_file_path)
	}

	if file_nums >= (this.nMaxRollupNums + 1) {
		old_file_path := fmt.Sprintf("%s/%s.%d", this.strFileDir, this.strFileName, file_nums)
		new_file_path := fmt.Sprintf("%s/%s", this.strFileDir, this.strFileName)
		os.Rename(old_file_path, new_file_path)
	}

	new_file_path := fmt.Sprintf("%s/%s", this.strFileDir, this.strFileName)
	this.file.Close()
	this.file, _ = os.OpenFile(new_file_path, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0744)

	return
}

func (this filelogger) checkLogFiles() int {
	var i int
	for i = 0; i <= this.nMaxRollupNums; i++ {
		file_full_path := fmt.Sprintf("%s/%s.%d", this.strFileDir, this.strFileName, i)
		if i == 0 {
			file_full_path = fmt.Sprintf("%s/%s", this.strFileDir, this.strFileName)
		}
		_, err := os.Stat(file_full_path)
		if err != nil {
			break
		}
	}

	return i
}

func (this *filelogger) doWrite(log_data []byte) {
	defer unlock(this.file_lock)

	lock(this.file_lock)

	// 打开文件,写入数据
	this.file.Write(log_data)
	//this.file.Sync()
	file_size, _ := this.file.Seek(0, os.SEEK_END)
	if file_size < int64(this.nMaxFileSize) {
		return
	}

	// 超过文件最大限制
	this.dupFile()
}

func (this *filelogger) DoLog(Logv chan *logtask) {
	defer this.file.Close()
	defer this.file_lock.Close()
	defer close(Logv)

	for {
		LogTask := <-Logv

		// 写入数据
		if LogTask.nLogType == LOG_TYPE {
			if LogTask.nLogLevel <= this.nLogLevel {
				this.doWrite(LogTask.LogData)
			}
		} else if LogTask.nLogType == EXIT_TYPE {
			// 收到退出信号
			break
		}
	}

	//此时操作写完所有的数据
	for {
		select {
		case LogTask := <-Logv:
			this.doWrite(LogTask.LogData)
		default: // 通道已经空,输出“退出”信号
			this.StopChannel <- 1
			return
		}
	}

}

func (this *filelogger) LogData(nLogLevel int, strLogPrex string, strFormat string, args ...interface{}) {
	var Task logtask

	strLogPrex += "|"
	strLogData := fmt.Sprintf(strFormat, args...)
	strLogData = strLogPrex + strLogData
	strLogData += "\r\n"
	Task.LogData = []byte(strLogData)
	Task.nLogLevel = nLogLevel
	Task.nLogType = LOG_TYPE

	this.LogChannel <- &Task

}

func createFilelogger(strFilePath string,
	nLogLevel int,
	nMaxFileSize int,
	nMaxRollupNums int) (*filelogger, error) {

	var strFileName, strDir string

	func(strFilePath string) {
		var i int
		nLen := len(strFilePath)
		for i = int(nLen) - 1; i >= 0; i-- {
			if strFilePath[i] == '/' {
				break
			}
		}
		if i != 0 {
			strDir = strFilePath[0:i]
		}
		strFileName = strFilePath[i+1:]

	}(strFilePath)

	// 创建文件锁，用来互斥操作日志文件
	file_lock_path := fmt.Sprintf("%s/.lock", strDir)
	file_lock, _ := os.OpenFile(file_lock_path, os.O_CREATE|os.O_RDWR, 0644)

	lock(file_lock)

	// 创建加锁
	ptrFile, Err := os.OpenFile(strFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0744)
	if Err != nil {
		unlock(file_lock)
		file_lock.Close()

		return nil, Err
	}

	unlock(file_lock)

	ptrFileLogger := &filelogger{ptrFile,
		file_lock,
		strDir,
		strFileName,
		nMaxRollupNums,
		nMaxFileSize,
		nLogLevel,
		make(chan *logtask, 1024),
		make(chan int, 1)}

	go ptrFileLogger.DoLog(ptrFileLogger.LogChannel)

	return ptrFileLogger, nil
}

func (this *filelogger) Destory() {
	var task logtask

	task.nLogType = EXIT_TYPE
	task.nLogLevel = 0
	this.LogChannel <- &task
	<-this.StopChannel
}

func lock(file_lock *os.File) {
	fd := file_lock.Fd()
	syscall.Syscall(syscall.SYS_FLOCK, 2, uintptr(fd), syscall.LOCK_EX)

}

func unlock(file_lock *os.File) {
	fd := file_lock.Fd()
	syscall.Syscall(syscall.SYS_FLOCK, 2, uintptr(fd), syscall.LOCK_UN)
}
