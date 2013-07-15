#Hosts-simple.pp

    # Host type reference:
    # http://docs.puppetlabs.com/references/stable/type.html#host

    if $hostname =~ /\bcassandra-(\d+)-(\d+)-(\d+)/ {
         $seed = "cassandra-${1}-${2}-1"
         $exp = "$2"
         notify {"Seed: $1, Cluster id: $2, Unique id: $3" :
        }

        file {'yaml':
              ensure  => file,
              path    => '/home/ubuntu/apache-cassandra-1.2.0-beta2-SNAPSHOT/conf/cassandra.yaml',
              mode    => 0755,
              content => template('/home/ubuntu/cassandra.yaml.erb'),
            }
   }

   if $hostname =~ /\bhadoop-(\d+)-(\d+)/ {
         $namenode = "hadoop-${1}-1"
         $exp = "$2"
         notify {"Seed: $1, Unique id: $2" :
        }

        file {'core-site':
              ensure  => file,
              path    => '/home/ubuntu/hadoop-common-3.0.0-SNAPSHOT/etc/hadoop/core-site.xml',
              mode    => 0755,
              content => template('/home/ubuntu/core-site.xml.erb'),
            }

        file {'hdfs-site':
              ensure  => file,
              path    => '/home/ubuntu/hadoop-hdfs-3.0.0-SNAPSHOT/etc/hadoop/hdfs-site.xml',
              mode    => 0755,
              content => template('/home/ubuntu/hdfs-site.xml.erb'),
            }

        file {'yarn-site':
              ensure  => file,
              path    => '/home/ubuntu/hadoop-hdfs-3.0.0-SNAPSHOT/etc/hadoop/yarn-site.xml',
              mode    => 0755,
              content => template('/home/ubuntu/yarn-site.xml.erb'),
            }

        file {'mapred-site':
              ensure  => file,
              path    => '/home/ubuntu/hadoop-hdfs-3.0.0-SNAPSHOT/etc/hadoop/mapred-site.xml',
              mode    => 0755,
              content => template('/home/ubuntu/mapred-site.xml.erb'),
            }
   }

   if $hostname =~ /\bstreaming-(\d+)-(\d+)/ {
         $server = "streaming-${1}-1"
         $exp = "$2"
         notify {"Server: $1, Unique id: $2" :
        }

        file {'run.xml':
              ensure  => file,
              path    => '/home/ubuntu/streaming-release/faban-streaming/streaming/deploy/run.xml',
              mode    => 0755,
              content => template('/home/ubuntu/run.xml.erb'),
            }
   


  if $hostname =~ /\web-client-(\d+)-(\d+)/ {
         $client = "web-client-${1}-1"
         $exp = "$2"
         notify {"Seed: $1, Unique id: $2" :
        }

        file {'core-site':
              ensure  => file,
              path    => '/home/ubuntu/hadoop-common-3.0.0-SNAPSHOT/etc/hadoop/core-site.xml',
              mode    => 0755,
              content => template('/home/ubuntu/core-site.xml.erb'),
            }

        file {'hdfs-site':
              ensure  => file,
              path    => '/home/ubuntu/hadoop-hdfs-3.0.0-SNAPSHOT/etc/hadoop/hdfs-site.xml',
              mode    => 0755,
              content => template('/home/ubuntu/hdfs-site.xml.erb'),
            }

    if $hostname =~ /\web-database-(\d+)-(\d+)/ {
         $client = "web-databse-${1}-2"
         $exp = "$2"
         notify {"Seed: $1, Unique id: $2" :
        }

        file {'core-site':
              ensure  => file,
              path    => '/home/ubuntu/hadoop-common-3.0.0-SNAPSHOT/etc/hadoop/core-site.xml',
              mode    => 0755,
              content => template('/home/ubuntu/core-site.xml.erb'),
            }

        file {'hdfs-site':
              ensure  => file,
              path    => '/home/ubuntu/hadoop-hdfs-3.0.0-SNAPSHOT/etc/hadoop/hdfs-site.xml',
              mode    => 0755,
              content => template('/home/ubuntu/hdfs-site.xml.erb'),
            }
    if $hostname =~ /\web-server-(\d+)-(\d+)/ {
         $client = "web-server-${1}-3"
         $exp = "$2"
         notify {"Seed: $1, Unique id: $2" :
        }

        file {'core-site':
              ensure  => file,
              path    => '/home/ubuntu/hadoop-common-3.0.0-SNAPSHOT/etc/hadoop/core-site.xml',
              mode    => 0755,
              content => template('/home/ubuntu/core-site.xml.erb'),
            }

        file {'hdfs-site':
              ensure  => file,
              path    => '/home/ubuntu/hadoop-hdfs-3.0.0-SNAPSHOT/etc/hadoop/hdfs-site.xml',
              mode    => 0755,
              content => template('/home/ubuntu/hdfs-site.xml.erb'),
            }


  }
