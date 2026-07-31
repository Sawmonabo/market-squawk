!macro NSIS_HOOK_PREUNINSTALL
  ${If} $UpdateMode <> 1
    !insertmacro CheckIfAppIsRunning "${MAINBINARYNAME}.exe" "${PRODUCTNAME}"
    StrCpy $0 -1
    ClearErrors
    ExecWait '"$INSTDIR\${MAINBINARYNAME}.exe" --native-uninstall' $0
    ${If} ${Errors}
    ${OrIf} $0 <> 0
      SetErrorLevel 1
      Quit
    ${EndIf}
  ${EndIf}
!macroend
