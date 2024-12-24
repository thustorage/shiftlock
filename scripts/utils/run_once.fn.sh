# Run a normal experiment once.
# Signature: run_once <CMD>
run_once() {
    local TMUX_SERVER="shiftlock-server"
    local SERVER_CMD="cargo run --release --bin server"
    local SCRIPT_DIR=$(dirname $(readlink -f "$0"))

    local CMD="$1"

    # Launch the server.
    tmux kill-session -t $TMUX_SERVER >> /dev/null 2>&1
    tmux new-session -s $TMUX_SERVER -n server -d
    tmux send-keys -t $TMUX_SERVER "$SERVER_CMD" C-m

    # Wait until server is ready.
    RETRY_CNT=0
    while [[ $RETRY_CNT -le 90 ]]; do
        if [[ $(tmux capture-pane -t $TMUX_SERVER -p | grep "Listening on") ]]; then
            RETRY_CNT=99999
            break
        fi
        sleep 1
        RETRY_CNT=$((RETRY_CNT+1))
    done

    if [[ $RETRY_CNT -eq 99999 ]]; then
        echo "Server is ready."
    else
        tmux send-keys -t $TMUX_SERVER C-c
        tmux send-keys -t $TMUX_SERVER C-c
        sleep 1
        tmux kill-session -t $TMUX_SERVER
        $SCRIPT_DIR/../utils/kill.sh

        echo "Failed to start server!"
        return 1
    fi

    # Run the client.
    eval "$CMD"

    # Stop the server.
    tmux send-keys -t $TMUX_SERVER C-c
    tmux send-keys -t $TMUX_SERVER C-c
    sleep 1
    tmux kill-session -t $TMUX_SERVER >> /dev/null 2>&1

    # Cleanup.
    $SCRIPT_DIR/../utils/kill.sh >> /dev/null 2>&1
    sleep 3

    return 0
}
