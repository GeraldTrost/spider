rem @ECHO OFF

:LOOP
START /MIN /W node spider %1 %2 %3 %4 | ECHO.
GOTO LOOP
