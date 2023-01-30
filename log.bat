@echo off 

if "%1" == "h" goto begin 
mshta vbscript:createobject("wscript.shell").run("%~nx0 h",0)(window.close)&&exit 


:begin

cd 整合機器人V{version}

explorer config\log
