package aihelper

import (
	"GopherAI/common/rabbitmq"
	"GopherAI/model"
	"GopherAI/utils"
	"context"
	"fmt"
	"sync"

	"github.com/cloudwego/eino/schema"
)

const (
	// RetainRecentCount 总结后保留最近多少条消息在内存中
	RetainRecentCount = 5
)

// AIHelper AI助手结构体，包含消息历史和AI模型
type AIHelper struct {
	model    AIModel
	messages []*model.Message
	mu       sync.RWMutex
	//一个会话绑定一个AIHelper
	SessionID string
	saveFunc  func(*model.Message) (*model.Message, error)
}

// NewAIHelper 创建新的AIHelper实例
func NewAIHelper(model_ AIModel, SessionID string) *AIHelper {
	return &AIHelper{
		model:    model_,
		messages: make([]*model.Message, 0),
		//异步推送到消息队列中
		saveFunc: func(msg *model.Message) (*model.Message, error) {
			data := rabbitmq.GenerateMessageMQParam(msg.SessionID, msg.Content, msg.UserName, msg.IsUser)
			err := rabbitmq.RMQMessage.Publish(data)
			return msg, err
		},
		SessionID: SessionID,
	}
}

// addMessage 添加消息到内存中并调用自定义存储函数
func (a *AIHelper) AddMessage(Content string, UserName string, IsUser bool, Save bool) {
	userMsg := model.Message{
		SessionID: a.SessionID,
		Content:   Content,
		UserName:  UserName,
		IsUser:    IsUser,
	}
	a.messages = append(a.messages, &userMsg)
	if Save {
		a.saveFunc(&userMsg)
	}
}

// SaveMessage 保存消息到数据库（通过回调函数避免循环依赖）
// 通过传入func，自己调用外部的保存函数，即可支持同步异步等多种策略
func (a *AIHelper) SetSaveFunc(saveFunc func(*model.Message) (*model.Message, error)) {
	a.saveFunc = saveFunc
}

// GetMessages 获取所有消息历史
func (a *AIHelper) GetMessages() []*model.Message {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]*model.Message, len(a.messages))
	copy(out, a.messages)
	return out
}

// 同步生成
func (a *AIHelper) GenerateResponse(userName string, ctx context.Context, userQuestion string) (*model.Message, error) {

	//调用存储函数
	a.AddMessage(userQuestion, userName, true, true)

	a.mu.RLock()
	//将model.Message转化成schema.Message
	messages := utils.ConvertToSchemaMessages(a.messages)
	a.mu.RUnlock()

	//调用模型生成回复
	schemaMsg, err := a.model.GenerateResponse(ctx, messages)
	if err != nil {
		return nil, err
	}

	//将schema.Message转化成model.Message
	modelMsg := utils.ConvertToModelMessage(a.SessionID, userName, schemaMsg)

	//调用存储函数
	a.AddMessage(modelMsg.Content, userName, false, true)

	return modelMsg, nil
}

// 流式生成
func (a *AIHelper) StreamResponse(userName string, ctx context.Context, cb StreamCallback, userQuestion string) (*model.Message, error) {

	//调用存储函数
	a.AddMessage(userQuestion, userName, true, true)

	a.mu.RLock()
	messages := utils.ConvertToSchemaMessages(a.messages)
	a.mu.RUnlock()

	content, err := a.model.StreamResponse(ctx, messages, cb)
	if err != nil {
		return nil, err
	}
	//转化成model.Message
	modelMsg := &model.Message{
		SessionID: a.SessionID,
		UserName:  userName,
		Content:   content,
		IsUser:    false,
	}

	//调用存储函数
	a.AddMessage(modelMsg.Content, userName, false, true)

	return modelMsg, nil
}

// GetModelType 获取模型类型
func (a *AIHelper) GetModelType() string {
	return a.model.GetModelType()
}

// GetSessionID 获取会话ID
func (a *AIHelper) GetSessionID() string {
	return a.SessionID
}

// GenerateSummary 生成摘要（用于调用AI生成摘要文本，不影响主对话历史）
func (a *AIHelper) GenerateSummary(ctx context.Context, prompt string) (*model.Message, error) {
	schemaMessages := []*schema.Message{
		{
			Role:    schema.User,
			Content: prompt,
		},
	}

	schemaMsg, err := a.model.GenerateResponse(ctx, schemaMessages)
	if err != nil {
		return nil, err
	}

	return &model.Message{
		SessionID: a.SessionID,
		Content:   schemaMsg.Content,
		IsUser:    false,
	}, nil
}

// RemoveOldMessages 移除旧消息（用于总结后清理）
func (a *AIHelper) RemoveOldMessages(keepCount int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if keepCount <= 0 {
		keepCount = 5 // 默认保留最近5条
	}

	if len(a.messages) > keepCount {
		a.messages = a.messages[len(a.messages)-keepCount:]
	}
}

// TrimMessages 裁剪消息到指定数量
func (a *AIHelper) TrimMessages(maxCount int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.messages) > maxCount {
		a.messages = a.messages[len(a.messages)-maxCount:]
	}
}

// GetRecentMessages 获取最近N条消息
func (a *AIHelper) GetRecentMessages(count int) []*model.Message {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if count <= 0 || count > len(a.messages) {
		count = len(a.messages)
	}

	start := len(a.messages) - count
	out := make([]*model.Message, count)
	copy(out, a.messages[start:])
	return out
}

// CallModel 直接调用底层AI模型（用于生成摘要等辅助任务）
func (a *AIHelper) CallModel(ctx context.Context, messages []map[string]interface{}) (*schema.Message, error) {
	// 将 map 转换为 schema.Message
	schemaMsgs := make([]*schema.Message, 0, len(messages))
	for _, m := range messages {
		role := schema.Assistant
		if r, ok := m["role"].(string); ok {
			switch r {
			case "user":
				role = schema.User
			case "system":
				role = schema.System
			}
		}
		schemaMsgs = append(schemaMsgs, &schema.Message{
			Role:    role,
			Content: m["content"].(string),
		})
	}
	return a.model.GenerateResponse(ctx, schemaMsgs)
}

// GetAllMessages 获取所有消息（不加锁版本，供内部使用）
func (a *AIHelper) GetAllMessagesUnsafe() []*model.Message {
	return a.messages
}

// GenerateResponseWithSummaries 使用历史摘要生成回复
func (a *AIHelper) GenerateResponseWithSummaries(userName string, ctx context.Context, currentQuestion string, summaries []*model.ConversationSummary) (*model.Message, error) {
	// 构建完整的消息列表
	var messages []*schema.Message

	// 1. 添加历史摘要（从旧到新）
	for _, s := range summaries {
		summaryText := fmt.Sprintf("[之前对话摘要]: %s\n[关键词]: %s", s.SummaryText, s.Keywords)
		messages = append(messages, &schema.Message{
			Role:    schema.System,
			Content: summaryText,
		})
	}

	// 2. 添加最近的对话（保证短期记忆）
	recentMessages := a.GetRecentMessages(RetainRecentCount)
	for _, msg := range recentMessages {
		role := schema.User
		if !msg.IsUser {
			role = schema.Assistant
		}
		messages = append(messages, &schema.Message{
			Role:    role,
			Content: msg.Content,
		})
	}

	// 3. 添加当前问题
	messages = append(messages, &schema.Message{
		Role:    schema.User,
		Content: currentQuestion,
	})

	// 调用模型生成回复
	schemaMsg, err := a.model.GenerateResponse(ctx, messages)
	if err != nil {
		return nil, err
	}

	// 转换并保存
	modelMsg := utils.ConvertToModelMessage(a.SessionID, userName, schemaMsg)
	a.AddMessage(modelMsg.Content, userName, false, true)

	return modelMsg, nil
}

// StreamResponseWithSummaries 使用历史摘要流式生成回复
func (a *AIHelper) StreamResponseWithSummaries(userName string, ctx context.Context, currentQuestion string, cb StreamCallback, summaries []*model.ConversationSummary) (*model.Message, error) {
	// 构建完整的消息列表
	var messages []*schema.Message

	// 1. 添加历史摘要（从旧到新）
	for _, s := range summaries {
		summaryText := fmt.Sprintf("[之前对话摘要]: %s\n[关键词]: %s", s.SummaryText, s.Keywords)
		messages = append(messages, &schema.Message{
			Role:    schema.System,
			Content: summaryText,
		})
	}

	// 2. 添加最近的对话（保证短期记忆）
	recentMessages := a.GetRecentMessages(RetainRecentCount)
	for _, msg := range recentMessages {
		role := schema.User
		if !msg.IsUser {
			role = schema.Assistant
		}
		messages = append(messages, &schema.Message{
			Role:    role,
			Content: msg.Content,
		})
	}

	// 3. 添加当前问题
	messages = append(messages, &schema.Message{
		Role:    schema.User,
		Content: currentQuestion,
	})

	// 调用模型流式生成回复
	content, err := a.model.StreamResponse(ctx, messages, cb)
	if err != nil {
		return nil, err
	}

	// 转换并保存
	modelMsg := &model.Message{
		SessionID: a.SessionID,
		UserName:  userName,
		Content:   content,
		IsUser:    false,
	}
	a.AddMessage(modelMsg.Content, userName, false, true)

	return modelMsg, nil
}
