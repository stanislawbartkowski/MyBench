source $FUNCTIONSRC
setenv

run() {
    read -r EXECORES <<< `getconfvar spark.executor.cores`
    verify_pars EXECORES 

}

run