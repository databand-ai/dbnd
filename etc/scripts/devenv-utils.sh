#!/usr/bin/env bash

# Source this script to use functions
# make sure you run it at bash
#    * 'read' doesn't behave the same way at zsh



# usually it's sourced, do not enable this flags, it will kill parent shell
#set -e
#set -o xtrace


_test(){
    local venv_target_name=$1
    echo "VAR = $venv_target_name"
}

_validate_python_venv_name() {
    local venv_target_name=$1
    local current_venv_name
    current_venv_name=$(basename ${VIRTUAL_ENV})
    if [[ "$current_venv_name" != *"${venv_target_name}" ]];
    then
        if [[ -z "${VIRTUAL_ENV}" ]];
        then
            echo "Virtual env is not activated, activate ${venv_target_name}";
        else
            echo "Looks like wrong virtual env '$current_venv_name' is activated, '${venv_target_name}' is expected";
        fi
        exit 1
# there is no good way for backward compatibility for read command /bash and /zsh
#        read -p "Are you sure you want to proceed? " -n 1 -r;
#        [[ "Yy" != *"$REPLY"* ]] && exit 1;
    fi
    echo
    echo Virtual env check passed, current venv is $current_venv_name
}

_create_virtualenv(){
    local venv_target_name=$1
    local python_version
    python_version=$(python -c "import sys; print('{0}.{1}.{2}'.format(*sys.version_info[:3]))")
    echo "Virtual env '${venv_target_name}' with python version ${python_version} from a current shell is going to be created"

    # there is no good way for backward compatibility for read command /bash and /zsh
    pyenv virtualenv ${venv_target_name}
}
