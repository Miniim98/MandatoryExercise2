// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.18.0
// source: proto/nodes.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type AccesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp    *Timestamp `protobuf:"bytes,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	RequestingId int32      `protobuf:"varint,2,opt,name=RequestingId,proto3" json:"RequestingId,omitempty"`
}

func (x *AccesRequest) Reset() {
	*x = AccesRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_nodes_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AccesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AccesRequest) ProtoMessage() {}

func (x *AccesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_proto_nodes_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AccesRequest.ProtoReflect.Descriptor instead.
func (*AccesRequest) Descriptor() ([]byte, []int) {
	return file_proto_nodes_proto_rawDescGZIP(), []int{0}
}

func (x *AccesRequest) GetTimestamp() *Timestamp {
	if x != nil {
		return x.Timestamp
	}
	return nil
}

func (x *AccesRequest) GetRequestingId() int32 {
	if x != nil {
		return x.RequestingId
	}
	return 0
}

type AccessResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp       *Timestamp `protobuf:"bytes,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	ResponseGranted bool       `protobuf:"varint,2,opt,name=responseGranted,proto3" json:"responseGranted,omitempty"`
}

func (x *AccessResponse) Reset() {
	*x = AccessResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_nodes_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AccessResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AccessResponse) ProtoMessage() {}

func (x *AccessResponse) ProtoReflect() protoreflect.Message {
	mi := &file_proto_nodes_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AccessResponse.ProtoReflect.Descriptor instead.
func (*AccessResponse) Descriptor() ([]byte, []int) {
	return file_proto_nodes_proto_rawDescGZIP(), []int{1}
}

func (x *AccessResponse) GetTimestamp() *Timestamp {
	if x != nil {
		return x.Timestamp
	}
	return nil
}

func (x *AccessResponse) GetResponseGranted() bool {
	if x != nil {
		return x.ResponseGranted
	}
	return false
}

type Timestamp struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Events int32 `protobuf:"varint,1,opt,name=events,proto3" json:"events,omitempty"`
}

func (x *Timestamp) Reset() {
	*x = Timestamp{}
	if protoimpl.UnsafeEnabled {
		mi := &file_proto_nodes_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Timestamp) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Timestamp) ProtoMessage() {}

func (x *Timestamp) ProtoReflect() protoreflect.Message {
	mi := &file_proto_nodes_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Timestamp.ProtoReflect.Descriptor instead.
func (*Timestamp) Descriptor() ([]byte, []int) {
	return file_proto_nodes_proto_rawDescGZIP(), []int{2}
}

func (x *Timestamp) GetEvents() int32 {
	if x != nil {
		return x.Events
	}
	return 0
}

var File_proto_nodes_proto protoreflect.FileDescriptor

var file_proto_nodes_proto_rawDesc = []byte{
	0x0a, 0x11, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x6e, 0x6f, 0x64, 0x65, 0x73, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x05, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x62, 0x0a, 0x0c, 0x41, 0x63,
	0x63, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x2e, 0x0a, 0x09, 0x74, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x52,
	0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x22, 0x0a, 0x0c, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x49, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05,
	0x52, 0x0c, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x49, 0x64, 0x22, 0x6a,
	0x0a, 0x0e, 0x41, 0x63, 0x63, 0x65, 0x73, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x2e, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x54, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
	0x12, 0x28, 0x0a, 0x0f, 0x72, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x47, 0x72, 0x61, 0x6e,
	0x74, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0f, 0x72, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x47, 0x72, 0x61, 0x6e, 0x74, 0x65, 0x64, 0x22, 0x23, 0x0a, 0x09, 0x54, 0x69,
	0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x12, 0x16, 0x0a, 0x06, 0x65, 0x76, 0x65, 0x6e, 0x74,
	0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05, 0x52, 0x06, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x73, 0x32,
	0x44, 0x0a, 0x03, 0x44, 0x4d, 0x45, 0x12, 0x3d, 0x0a, 0x0d, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
	0x74, 0x41, 0x63, 0x63, 0x65, 0x73, 0x73, 0x12, 0x13, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e,
	0x41, 0x63, 0x63, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x15, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x41, 0x63, 0x63, 0x65, 0x73, 0x73, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x22, 0x00, 0x42, 0x36, 0x5a, 0x34, 0x68, 0x74, 0x74, 0x70, 0x73, 0x3a, 0x2f,
	0x2f, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x4d, 0x69, 0x6e, 0x69,
	0x69, 0x6d, 0x39, 0x38, 0x2f, 0x4d, 0x61, 0x6e, 0x64, 0x61, 0x74, 0x6f, 0x72, 0x79, 0x45, 0x78,
	0x65, 0x72, 0x63, 0x69, 0x73, 0x65, 0x32, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_proto_nodes_proto_rawDescOnce sync.Once
	file_proto_nodes_proto_rawDescData = file_proto_nodes_proto_rawDesc
)

func file_proto_nodes_proto_rawDescGZIP() []byte {
	file_proto_nodes_proto_rawDescOnce.Do(func() {
		file_proto_nodes_proto_rawDescData = protoimpl.X.CompressGZIP(file_proto_nodes_proto_rawDescData)
	})
	return file_proto_nodes_proto_rawDescData
}

var file_proto_nodes_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_proto_nodes_proto_goTypes = []interface{}{
	(*AccesRequest)(nil),   // 0: proto.AccesRequest
	(*AccessResponse)(nil), // 1: proto.AccessResponse
	(*Timestamp)(nil),      // 2: proto.Timestamp
}
var file_proto_nodes_proto_depIdxs = []int32{
	2, // 0: proto.AccesRequest.timestamp:type_name -> proto.Timestamp
	2, // 1: proto.AccessResponse.timestamp:type_name -> proto.Timestamp
	0, // 2: proto.DME.RequestAccess:input_type -> proto.AccesRequest
	1, // 3: proto.DME.RequestAccess:output_type -> proto.AccessResponse
	3, // [3:4] is the sub-list for method output_type
	2, // [2:3] is the sub-list for method input_type
	2, // [2:2] is the sub-list for extension type_name
	2, // [2:2] is the sub-list for extension extendee
	0, // [0:2] is the sub-list for field type_name
}

func init() { file_proto_nodes_proto_init() }
func file_proto_nodes_proto_init() {
	if File_proto_nodes_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_proto_nodes_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AccesRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_nodes_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AccessResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_proto_nodes_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Timestamp); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_proto_nodes_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_proto_nodes_proto_goTypes,
		DependencyIndexes: file_proto_nodes_proto_depIdxs,
		MessageInfos:      file_proto_nodes_proto_msgTypes,
	}.Build()
	File_proto_nodes_proto = out.File
	file_proto_nodes_proto_rawDesc = nil
	file_proto_nodes_proto_goTypes = nil
	file_proto_nodes_proto_depIdxs = nil
}
