docker rmi -f $(docker images -f dangling=true -q)

# docker system prune -a
# sudo aa-remove-unknown
