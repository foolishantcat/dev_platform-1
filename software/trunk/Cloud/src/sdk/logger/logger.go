package logger

import (
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
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
	//file_lock      *os.File
	strFileDir     string
	strFileName    string
	nMaxRollupNums int
	nMaxFileSize   int
	nLogLevel      int
}

func (this *filelogger) dupFile() {
	var file_name string
	var new_file_path string
	var old_file_path string

	// 先保存为tmp文件
	new_file_path = fmt.Sprintf("%s/%s.tmp", this.strFileDir, this.strFileName)
	old_file_path = fmt.Sprintf("%s/%s", this.strFileDir, this.strFileName)

	err := os.Rename(old_file_path, new_file_path)
	if err != nil {
		fmt.Printf("Rename Failed!OldFilePath=%s,NewFilePath=%s,ErrString=%s",
			old_file_path,
			new_file_path,
			err.Error())

		return
	}

	if this.nMaxRollupNums > 0 {
		// 删除最久的文件
		file_name = fmt.Sprintf("%s/%s.%d",
			this.strFileDir,
			this.strFileName,
			this.nMaxRollupNums)

		fmt.Printf("RmoveFile=%s\n", file_name)

		os.Remove(file_name)
	}

	for i := this.nMaxRollupNums; i > 0; i-- {

		new_file_path = fmt.Sprintf("%s/%s.%d", this.strFileDir, this.strFileName, i)
		old_file_path = fmt.Sprintf("%s/%s.%d", this.strFileDir, this.strFileName, i-1)
		fmt.Printf("NewFile=%s,OldFile=%s\n", new_file_path, old_file_path)
		os.Rename(old_file_path, new_file_path)
	}

	new_file_path = fmt.Sprintf("%s/%s.0", this.strFileDir, this.strFileName)
	old_file_path = fmt.Sprintf("%s/%s.tmp", this.strFileDir, this.strFileName)
	os.Rename(old_file_path, new_file_path)
}

func (this filelogger) checkLogFiles() int {
	var i int
	for i = 0; i < this.nMaxRollupNums; i++ {
		file_full_path := fmt.Sprintf("%s/%s.%d", this.strFileDir, this.strFileName, i)
		_, err := os.Stat(file_full_path)
		if err != nil {
			break
		}

		log.Printf("has file_path=%s", file_full_path)
	}

	return i + 1
}

func (this *filelogger) LogData(nLogLevel int, strLogPrex string, strFormat string, args ...interface{}) {

	// 首先判断文件的大小
	var dir_name string
	var file *os.File
	var size int

	for i := 0; i < len(this.strFileDir); i++ {
		if this.strFileDir[i] == '/' {
			// 判断文件夹是否存在
			file_inf, err := os.Stat(dir_name)
			if err != nil {
				// 文件夹不存在,创建之
				os.Mkdir(dir_name, os.ModeDir|0744)
				log.Fatal("Create Dir Failed!")
				return
			}

			if !file_inf.IsDir() {
				// 存在同名文件,直接返回
				log.Fatal("File Exist!Path=", dir_name)
				return
			}
		}
		dir_name += this.strFileDir[i : i+1]
	}

	file_path := fmt.Sprintf("%s/%s", this.strFileDir, this.strFileName)
	file_inf, err := os.Stat(file_path)
	if err != nil { // 不存在该文件
		size = 0

	} else {
		size = int(file_inf.Size())
	}

	if size > this.nMaxFileSize {

		fmt.Printf("SeekSize=%d\n", size)

		// 关闭文件
		this.dupFile()

	}
	file, err = os.OpenFile(file_path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0744)
	if err != nil {
		log.Fatal("OpenFile Failed!ErrString=%s", err.Error())
	}

	strLogPrex += "|"
	strLogData := fmt.Sprintf(strFormat, args...)
	strLogData = strLogPrex + strLogData
	strLogData += "\r\n"

	file.WriteString(strLogData)
	file.Close()

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

	// 创建文件锁
	/*
		lock_file_path := fmt.Sprintf("%s/.%s.lock", strDir, strFileName)
		file_lock, err := os.OpenFile(lock_file_path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0744)
		if err != nil {
			return nil, err
		}
	*/

	// 创建加锁
	ptrFileLogger := &filelogger{strDir,
		strFileName,
		nMaxRollupNums,
		nMaxFileSize,
		nLogLevel}

	return ptrFileLogger, nil
}

func (this *filelogger) Destory() {
}

//var mux sync.Mute
