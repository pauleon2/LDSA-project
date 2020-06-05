hadoop fs -rm /LDSA/dataset/files.csv;
hadoop fs -ls -R /LDSA/dataset | grep '.h5$'| python -c '
import sys
with open("/home/ubuntu/files.csv", "w") as myfile:
    for line in sys.stdin:
        r = line.strip("\n").split(None, 10)
        fn = r.pop()
        myfile.write(",".join(r) + ",\"" + fn.replace("\"", "\"\"") + "\"")
        myfile.write("\n")
' && hdfs dfs -put /home/ubuntu/files.csv /LDSA/dataset && rm /home/ubuntu/files.csv && echo "current list of files succesfully created saved on hdfs"
