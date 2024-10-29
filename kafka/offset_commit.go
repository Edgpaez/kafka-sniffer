package kafka

type OffsetCommitRequest struct {
	GroupId                   string
	GenerationIdOrMemberEpoch int32
	MemberId                  string
	Topics                    []TopicOffsetCommit

	Version int16
}

type TopicOffsetCommit struct {
	Name             string
	PartitionIndexes []TopicPartitionIndex
}
type TopicPartitionIndex struct {
	PartitionIndex       int32
	CommittedOffset      int64
	CommittedLeaderEpoch int32
	CommittedMetadata    *string
}

func (r *OffsetCommitRequest) Decode(pd PacketDecoder, version int16) error {
	r.Version = version

	if version <= 6 {
		GroupId, err := pd.getString()
		if err != nil {
			return err
		}
		r.GroupId = GroupId

		GenerationIdOrMemberEpoch, err := pd.getInt32()
		if err != nil {
			return err
		}
		r.GenerationIdOrMemberEpoch = GenerationIdOrMemberEpoch

		MemberId, err := pd.getString()
		if err != nil {
			return err
		}
		r.MemberId = MemberId

		topicLength, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		r.Topics = []TopicOffsetCommit{}
		for i := 0; i < topicLength; i++ {
			name, err := pd.getString()
			if err != nil {
				return err
			}

			partitionsLength, err := pd.getArrayLength()
			if err != nil {
				return err
			}
			partitions := []TopicPartitionIndex{}
			for j := 0; j < partitionsLength; j++ {
				partitionIndex, err := pd.getInt32()
				if err != nil {
					return err
				}
				committedOffset, err := pd.getInt64()
				if err != nil {
					return err
				}
				committedLeaderEpoch, err := pd.getInt32()
				if err != nil {
					return err
				}
				committedMetadata, err := pd.getNullableString()
				if err != nil {
					return err
				}

				partitions = append(partitions, TopicPartitionIndex{
					PartitionIndex:       partitionIndex,
					CommittedOffset:      committedOffset,
					CommittedLeaderEpoch: committedLeaderEpoch,
					CommittedMetadata:    committedMetadata,
				})
			}

			r.Topics = append(r.Topics, TopicOffsetCommit{
				Name:             name,
				PartitionIndexes: partitions,
			})
		}
	}

	return nil
}

func (r *OffsetCommitRequest) key() int16 {
	return 8
}

func (r *OffsetCommitRequest) version() int16 {
	return r.Version
}

func (r *OffsetCommitRequest) requiredVersion() Version {
	return MaxVersion
}

func (r *OffsetCommitRequest) CollectClientMetrics(srcHost string) {
}
